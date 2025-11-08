#include "numbers.hpp"

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
#include "generator_global_state.hpp"
#include "probability_distributions.hpp"
#include "rowid_generator.hpp"
#include "utils/client_context_decl.hpp"

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

using namespace duckdb;

namespace duckdb_faker {

namespace {
struct RandomIntFunctionData final : TableFunctionData {
    std::optional<int32_t> min;
    std::optional<int32_t> max;
    std::optional<ProbabilityDistribution::Type> distribution;
};

struct IntGeneratorGlobalState final : GeneratorGlobalState {
    explicit IntGeneratorGlobalState(const TableFunctionInitInput& input) : GeneratorGlobalState(input) {
    }
};

unique_ptr<FunctionData> RandomIntBind(ClientContext&, TableFunctionBindInput& input, vector<LogicalType>& return_types,
                                       vector<string>& names) {
    names.push_back("value");
    return_types.push_back(LogicalType::INTEGER);

    auto bind_data = make_uniq<RandomIntFunctionData>();
    if (input.named_parameters.contains("min")) {
        bind_data->min = input.named_parameters["min"].GetValue<int32_t>();
    }
    if (input.named_parameters.contains("max")) {
        bind_data->max = input.named_parameters["max"].GetValue<int32_t>();
    }

    if (bind_data->min.has_value() && bind_data->max.has_value() && bind_data->min.value() > bind_data->max.value()) {
        throw InvalidInputException("Minimum value must be less than or equal to maximum value");
    }

    if (input.named_parameters.contains("distribution")) {
        const std::string distribution_str = input.named_parameters["distribution"].GetValue<string>();
        const std::optional<ProbabilityDistribution::Type> distribution =
            ProbabilityDistribution::FromString(distribution_str);
        if (!distribution.has_value()) {
            throw InvalidInputException("Unknown probability distribution \"%s\"", distribution_str);
        }
        bind_data->distribution = distribution;
    }

    return bind_data;
}

unique_ptr<GlobalTableFunctionState> RandomIntGlobalInit(ClientContext&, TableFunctionInitInput& input) {
    return make_uniq<IntGeneratorGlobalState>(input);
}

void RandomIntExecute(ClientContext&, TableFunctionInput& input, DataChunk& output) {
    auto& state = input.global_state->Cast<IntGeneratorGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;
    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    if (state.column_indexes.value_idx.IsValid() && state.column_indexes.rowid_idx.IsValid()) {
        D_ASSERT(output.ColumnCount() == 2);
    } else {
        D_ASSERT(output.ColumnCount() == 1);
    }

    const auto& bind_data = input.bind_data->Cast<RandomIntFunctionData>();
    const int32_t min = bind_data.min.value_or(std::numeric_limits<int32_t>::min());
    const int32_t max = bind_data.max.value_or(std::numeric_limits<int32_t>::max());
    const ProbabilityDistribution::Type distribution =
        bind_data.distribution.value_or(ProbabilityDistribution::Type::UNIFORM);

    const optional_idx value_col_idx = state.column_indexes.value_idx;
    if (value_col_idx.IsValid()) {
        Vector& value_vector = output.data[value_col_idx.GetIndex()];
        D_ASSERT(value_vector.GetType().id() == LogicalTypeId::INTEGER);
        D_ASSERT(LogicalType(LogicalType::INTEGER).InternalType() == duckdb::GetTypeId<int32_t>());
        D_ASSERT(value_vector.GetVectorType() == VectorType::FLAT_VECTOR);
        int32_t* data = FlatVector::GetData<int32_t>(value_vector);

        // We only support one distribution for now
        if (distribution == ProbabilityDistribution::Type::UNIFORM) {
            for (idx_t row_idx = 0; row_idx < cardinality; row_idx++) {
                const int32_t random_num = faker::number::integer(min, max);
                data[row_idx] = random_num;
            }
        }
    }

    const auto rowid_col_idx = state.column_indexes.rowid_idx;
    if (rowid_col_idx.IsValid()) {
        rowid_generator::PopulateRowIdColumn(state.num_generated_rows, rowid_col_idx, output);
    }

    state.num_generated_rows += cardinality;
}
} // anonymous namespace

void RandomIntFunction::RegisterFunction(ExtensionLoader& loader) {
    TableFunction random_int_function("random_int", {}, RandomIntExecute, RandomIntBind, RandomIntGlobalInit);
    random_int_function.named_parameters["min"] = LogicalType::INTEGER;
    random_int_function.named_parameters["max"] = LogicalType::INTEGER;
    random_int_function.named_parameters["distribution"] = LogicalType::VARCHAR;
    random_int_function.projection_pushdown = true;
    random_int_function.get_virtual_columns = rowid_generator::GetVirtualColumns;
    random_int_function.get_row_id_columns = rowid_generator::GetRowIdColumns;
    loader.RegisterFunction(random_int_function);
}

} // namespace duckdb_faker
