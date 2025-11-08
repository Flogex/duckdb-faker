#include "booleans.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "faker-cxx/datatype.h"
#include "generator_global_state.hpp"
#include "rowid_generator.hpp"
#include "utils/client_context_decl.hpp"

#include <algorithm>
#include <optional>
#include <string>

using namespace duckdb;

namespace duckdb_faker {

namespace {
struct RandomBoolFunctionData final : TableFunctionData {
    std::optional<double> true_probability;
    // If true_probability is 0 or 1, we can return a constant value
    std::optional<bool> constant_value;
};

struct RandomBoolGlobalState final : GeneratorGlobalState {
    explicit RandomBoolGlobalState(const TableFunctionInitInput& input) : GeneratorGlobalState(input) {
    }
};

unique_ptr<FunctionData> RandomBoolBind(ClientContext&, TableFunctionBindInput& input,
                                        vector<LogicalType>& return_types, vector<string>& names) {
    names.push_back("value");
    return_types.push_back(LogicalType::BOOLEAN);

    auto bind_data = make_uniq<RandomBoolFunctionData>();

    if (input.named_parameters.contains("true_probability")) {
        const auto true_probability = input.named_parameters["true_probability"].GetValue<double>();

        if (true_probability < 0 || true_probability > 1) {
            throw InvalidInputException("true_probability must be between 0 and 1");
        }

        if (true_probability == 0) {
            bind_data->constant_value = false;
        } else if (true_probability == 1) {
            bind_data->constant_value = true;
        } else {
            bind_data->true_probability = true_probability;
        }
    }

    return bind_data;
}

unique_ptr<GlobalTableFunctionState> RandomBoolGlobalInit(ClientContext&, TableFunctionInitInput& input) {
    return make_uniq<RandomBoolGlobalState>(input);
}

void RandomBoolExecute(ClientContext&, TableFunctionInput& input, DataChunk& output) {
    auto& state = input.global_state->Cast<RandomBoolGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;
    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    if (state.column_indexes.value_idx.IsValid() && state.column_indexes.rowid_idx.IsValid()) {
        D_ASSERT(output.ColumnCount() == 2);
    } else {
        D_ASSERT(output.ColumnCount() == 1);
    }

    const auto& bind_data = input.bind_data->Cast<RandomBoolFunctionData>();
    const double true_probability = bind_data.true_probability.value_or(0.5);

    const optional_idx value_col_idx = state.column_indexes.value_idx;
    if (value_col_idx.IsValid()) {
        Vector& value_vector = output.data[value_col_idx.GetIndex()];
        D_ASSERT(value_vector.GetType().id() == LogicalTypeId::BOOLEAN);

        //TODO: Handle validity mask once NULLs are supported
        if (bind_data.constant_value.has_value()) {
            value_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
            bool* data = ConstantVector::GetData<bool>(value_vector);
            std::fill_n(data, cardinality, bind_data.constant_value.value());
        } else {
            D_ASSERT(value_vector.GetVectorType() == VectorType::FLAT_VECTOR);
            auto data = FlatVector::GetData<bool>(value_vector);
            for (idx_t row_idx = 0; row_idx < cardinality; row_idx++) {
                const bool random_bool = faker::datatype::boolean(true_probability);
                data[row_idx] = random_bool;
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

void RandomBoolFunction::RegisterFunction(ExtensionLoader& loader) {
    TableFunction random_bool_function("random_bool", {}, RandomBoolExecute, RandomBoolBind, RandomBoolGlobalInit);
    random_bool_function.named_parameters["true_probability"] = LogicalType::DOUBLE;
    random_bool_function.projection_pushdown = true;
    random_bool_function.get_virtual_columns = rowid_generator::GetVirtualColumns;
    random_bool_function.get_row_id_columns = rowid_generator::GetRowIdColumns;
    loader.RegisterFunction(random_bool_function);
}

} // namespace duckdb_faker
