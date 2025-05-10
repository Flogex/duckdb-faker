#include "strings.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/main/extension_util.hpp"
#include "faker-cxx/number.h"
#include "faker-cxx/string.h"
#include "generator_global_state.hpp"

#include <cstdint>

using namespace duckdb;

namespace duckdb_faker {

namespace {
struct RandomStringFunctionData final : TableFunctionData {
    std::optional<uint64_t> length;
    std::optional<uint64_t> min_length;
    std::optional<uint64_t> max_length;
};

struct RandomStringGlobalState final : GeneratorGlobalState {};

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

    return bind_data;
}

unique_ptr<GlobalTableFunctionState> RandomStringGlobalInit(ClientContext&, TableFunctionInitInput&) {
    return make_uniq<RandomStringGlobalState>();
}

uint64_t get_string_length(const RandomStringFunctionData& bind_data) {
    if (bind_data.length.has_value()) {
        return bind_data.length.value();
    }

    uint64_t min = 1;
    if (bind_data.min_length.has_value()) {
        min = bind_data.min_length.value();
    }

    /*
     * For small values, we still want to have a big-enough range.
     * For example, for minimum length 1, there should be strings generated
     * also for length 20.
     * For minimum length 100, the maximum length should still be in the same
     * order of magnitude, for example 200.
     */
    uint64_t max = 0;
    if (min < 10) {
        max = 20;
    } else if (min < UINT64_MAX / 2) {
        max = min * 2;
    } else {
        max = UINT64_MAX;
    }

    return faker::number::integer(min, max);
}

void RandomStringExecute(ClientContext&, TableFunctionInput& input, DataChunk& output) {
    D_ASSERT(output.ColumnCount() == 1);
    auto& state = input.global_state->Cast<RandomStringGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;

    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    const auto& bind_data = input.bind_data->Cast<RandomStringFunctionData>();

    for (idx_t idx = 0; idx < cardinality; idx++) {
        const auto string_length = get_string_length(bind_data);
        const auto casing = faker::string::StringCasing::Lower;
        const std::string random_string = faker::string::alpha(string_length, casing);
        output.SetValue(0, idx, Value(random_string));
    }

    state.num_generated_rows += cardinality;
}
} // anonymous namespace

void RandomStringFunction::RegisterFunction(DatabaseInstance& instance) {
    TableFunction random_string_function(
        "random_string", {}, RandomStringExecute, RandomStringBind, RandomStringGlobalInit);
    random_string_function.named_parameters["length"] = LogicalType::UBIGINT;
    random_string_function.named_parameters["min_length"] = LogicalType::UBIGINT;
    random_string_function.named_parameters["max_length"] = LogicalType::UBIGINT;
    ExtensionUtil::RegisterFunction(instance, random_string_function);
}

} // namespace duckdb_faker