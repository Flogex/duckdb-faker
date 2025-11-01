#include "numbers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "faker-cxx/number.h"
#include "generator_global_state.hpp"
#include "probability_distributions.hpp"

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

using namespace duckdb;

namespace duckdb {
class ClientContext;
}

namespace duckdb_faker {

namespace {
struct RandomIntFunctionData final : TableFunctionData {
    std::optional<int32_t> min;
    std::optional<int32_t> max;
    std::optional<ProbabilityDistribution::Type> distribution;
};

struct RandomIntGlobalState final : GeneratorGlobalState {};

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

unique_ptr<GlobalTableFunctionState> RandomIntGlobalInit(ClientContext&, TableFunctionInitInput&) {
    return make_uniq<RandomIntGlobalState>();
}

void RandomIntExecute(ClientContext&, TableFunctionInput& input, DataChunk& output) {
    D_ASSERT(output.ColumnCount() == 1);
    auto& state = input.global_state->Cast<RandomIntGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;

    const auto& bind_data = input.bind_data->Cast<RandomIntFunctionData>();
    const int32_t min = bind_data.min.value_or(std::numeric_limits<int32_t>::min());
    const int32_t max = bind_data.max.value_or(std::numeric_limits<int32_t>::max());
    const ProbabilityDistribution::Type distribution =
        bind_data.distribution.value_or(ProbabilityDistribution::Type::UNIFORM);

    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    if (distribution == ProbabilityDistribution::Type::UNIFORM) {
        for (idx_t idx = 0; idx < cardinality; idx++) {
            const int32_t random_num = faker::number::integer(min, max);
            output.SetValue(0, idx, Value(random_num));
        }
    }

    state.num_generated_rows += cardinality;
}
} // anonymous namespace

void RandomIntFunction::RegisterFunction(ExtensionLoader& loader) {
    TableFunction random_int_function("random_int", {}, RandomIntExecute, RandomIntBind, RandomIntGlobalInit);
    random_int_function.named_parameters["min"] = LogicalType::INTEGER;
    random_int_function.named_parameters["max"] = LogicalType::INTEGER;
    random_int_function.named_parameters["distribution"] = LogicalType::VARCHAR;
    loader.RegisterFunction(random_int_function);
}

} // namespace duckdb_faker
