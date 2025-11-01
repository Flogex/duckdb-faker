#include "strings.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "faker-cxx/number.h"
#include "faker-cxx/string.h"
#include "generator_global_state.hpp"
#include "rowid_generator.hpp"
#include "string_casing.hpp"
#include "utils/client_context_decl.hpp"

#include <cstdint>
#include <string>

using namespace duckdb;

namespace duckdb_faker {

namespace {
struct RandomStringFunctionData final : TableFunctionData {
    std::optional<uint64_t> length;
    std::optional<uint64_t> min_length;
    std::optional<uint64_t> max_length;
    std::optional<StringCasing> casing;
};

struct RandomStringGlobalState final : GeneratorGlobalState {
    explicit RandomStringGlobalState(const TableFunctionInitInput& input) : GeneratorGlobalState(input) {
    }
};

unique_ptr<FunctionData> RandomStringBind(ClientContext&, TableFunctionBindInput& input,
                                          vector<LogicalType>& return_types, vector<string>& names) {
    names.push_back("value");
    return_types.push_back(LogicalType::VARCHAR);

    auto bind_data = make_uniq<RandomStringFunctionData>();

    const auto& named_parameters = input.named_parameters;

    if (named_parameters.contains("length") &&
        (named_parameters.contains("min_length") || named_parameters.contains("max_length"))) {
        throw InvalidInputException("Can only specify either length or min_length/max_length");
    }

    if (named_parameters.contains("length")) {
        bind_data->length = named_parameters.at("length").GetValue<uint64_t>();
    }

    if (named_parameters.contains("min_length")) {
        bind_data->min_length = named_parameters.at("min_length").GetValue<uint64_t>();
    }

    if (named_parameters.contains("max_length")) {
        bind_data->max_length = named_parameters.at("max_length").GetValue<uint64_t>();
    }

    // Validate min_length and max_length if both are specified
    if (bind_data->min_length.has_value() && bind_data->max_length.has_value() &&
        bind_data->min_length.value() > bind_data->max_length.value()) {
        throw InvalidInputException("min_length cannot be greater than max_length");
    }

    if (named_parameters.contains("casing")) {
        const auto casing_str = named_parameters.at("casing").GetValue<string>();
        bind_data->casing = string_casing_from_string(casing_str);
        if (!bind_data->casing.has_value()) {
            throw InvalidInputException("casing must be one of: lower, upper, mixed");
        }
    }

    return bind_data;
}

unique_ptr<GlobalTableFunctionState> RandomStringGlobalInit(ClientContext&, TableFunctionInitInput& input) {
    return make_uniq<RandomStringGlobalState>(input);
}

uint64_t get_string_length(const RandomStringFunctionData& bind_data) {
    if (bind_data.length.has_value()) {
        return bind_data.length.value();
    }

    uint64_t min = 1;
    if (bind_data.min_length.has_value()) {
        min = bind_data.min_length.value();
    }

    uint64_t max;
    if (bind_data.max_length.has_value()) {
        max = bind_data.max_length.value();
    } else {
        /*
         * For small values, we still want to have a big-enough range.
         * For example, for minimum length 1, there should be strings generated
         * also for length 20.
         * For minimum length 100, the maximum length should still be in the same
         * order of magnitude, for example 200.
         */
        if (min < 10) {
            max = 20;
        } else if (min < UINT64_MAX / 2) {
            max = min * 2;
        } else {
            max = UINT64_MAX;
        }
    }

    return faker::number::integer(min, max);
}

void RandomStringExecute(ClientContext&, TableFunctionInput& input, DataChunk& output) {
    auto& state = input.global_state->Cast<RandomStringGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;
    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    if (state.column_indexes.value_idx.IsValid() && state.column_indexes.rowid_idx.IsValid()) {
        D_ASSERT(output.ColumnCount() == 2);
    } else {
        D_ASSERT(output.ColumnCount() == 1);
    }

    const auto& bind_data = input.bind_data->Cast<RandomStringFunctionData>();

    const optional_idx value_col_idx = state.column_indexes.value_idx;
    if (value_col_idx.IsValid()) {
        Vector value_vector = output.data[value_col_idx.GetIndex()];
        D_ASSERT(value_vector.GetType().id() == LogicalTypeId::VARCHAR);
        D_ASSERT(value_vector.GetVectorType() == VectorType::FLAT_VECTOR);

        const auto casing = to_faker_casing(bind_data.casing.value_or(StringCasing::Lower));
        for (idx_t row_idx = 0; row_idx < cardinality; row_idx++) {
            const auto string_length = get_string_length(bind_data);
            const std::string random_string = faker::string::alpha(string_length, casing);
            value_vector.SetValue(row_idx, Value(random_string));
        }
    }

    const auto rowid_col_idx = state.column_indexes.rowid_idx;
    if (rowid_col_idx.IsValid()) {
        rowid_generator::PopulateRowIdColumn(state.num_generated_rows, rowid_col_idx, output);
    }

    state.num_generated_rows += cardinality;
}
} // anonymous namespace

void RandomStringFunction::RegisterFunction(ExtensionLoader& loader) {
    TableFunction random_string_function(
        "random_string", {}, RandomStringExecute, RandomStringBind, RandomStringGlobalInit);
    random_string_function.named_parameters["length"] = LogicalType::UBIGINT;
    random_string_function.named_parameters["min_length"] = LogicalType::UBIGINT;
    random_string_function.named_parameters["max_length"] = LogicalType::UBIGINT;
    random_string_function.named_parameters["casing"] = LogicalType::VARCHAR;
    random_string_function.projection_pushdown = true;
    random_string_function.get_virtual_columns = rowid_generator::GetVirtualColumns;
    random_string_function.get_row_id_columns = rowid_generator::GetRowIdColumns;
    loader.RegisterFunction(random_string_function);
}

} // namespace duckdb_faker