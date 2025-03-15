#include "booleans.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "faker-cxx/datatype.h"
#include "generator_global_state.hpp"
#include "utils/client_context.hpp"

#include <optional>

using namespace duckdb;

namespace duckdb_faker {

namespace {
struct RandomBoolFunctionData final : TableFunctionData {
    std::optional<double> true_probability;
    // If true_probability is 0 or 1, we can return a constant value
    std::optional<bool> constant_value;
};

struct RandomBoolGlobalState final : GeneratorGlobalState { };

unique_ptr<FunctionData> RandomBoolBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                       vector<string> &names) {
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

unique_ptr<GlobalTableFunctionState> RandomBoolGlobalInit(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<RandomBoolGlobalState>();
}

void RandomBoolExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    D_ASSERT(output.ColumnCount() == 1);
    auto &state = input.global_state->Cast<RandomBoolGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;
    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    const auto &bind_data = input.bind_data->Cast<RandomBoolFunctionData>();
    const double true_probability = bind_data.true_probability.value_or(0.5);

    auto output_vector = output.data[0];
    D_ASSERT(output_vector.GetType().id() == LogicalTypeId::BOOLEAN);
    if (bind_data.constant_value.has_value()) {
        output_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
    }

    for (idx_t idx = 0; idx < cardinality; idx++) {
        const bool val = bind_data.constant_value.value_or(faker::datatype::boolean(true_probability));
        output_vector.SetValue(idx, Value(val));
    }

    state.num_generated_rows += cardinality;
}
} // anonymous namespace

void RandomBoolFunction::RegisterFunction(DatabaseInstance &instance) {
    TableFunction random_bool_function("random_bool", {}, RandomBoolExecute, RandomBoolBind, RandomBoolGlobalInit);
    random_bool_function.named_parameters["true_probability"] = LogicalType::DOUBLE;
    ExtensionUtil::RegisterFunction(instance, random_bool_function);
}

} // namespace duckdb_faker


