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
};

struct RandomStringGlobalState final : GeneratorGlobalState { };

unique_ptr<FunctionData> RandomStringBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                       vector<string> &names) {
    names.push_back("value");
    return_types.push_back(LogicalType::VARCHAR);

    auto bind_data = make_uniq<RandomStringFunctionData>();

    if (input.named_parameters.contains("length")) {
        bind_data->length = input.named_parameters["length"].GetValue<uint64_t>();
    }

    return bind_data;
}

unique_ptr<GlobalTableFunctionState> RandomStringGlobalInit(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<RandomStringGlobalState>();
}

void RandomStringExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    D_ASSERT(output.ColumnCount() == 1);
    auto &state = input.global_state->Cast<RandomStringGlobalState>();

    D_ASSERT(state.num_generated_rows <= state.max_generated_rows); // We don't want to underflow
    const auto num_remaining_rows = state.max_generated_rows - state.num_generated_rows;

    const idx_t cardinality = num_remaining_rows < STANDARD_VECTOR_SIZE ? num_remaining_rows : STANDARD_VECTOR_SIZE;
    output.SetCardinality(cardinality);

    const auto &bind_data = input.bind_data->Cast<RandomStringFunctionData>();
    const auto string_length = bind_data.length.value_or(faker::number::integer(1, 25));

    for (idx_t idx = 0; idx < cardinality; idx++) {
        const auto casing = faker::string::StringCasing::Lower;
        const std::string random_string = faker::string::alpha(string_length, casing);
        output.SetValue(0, idx, Value(random_string));
    }

    state.num_generated_rows += cardinality;
}
} // anonymous namespace

void RandomStringFunction::RegisterFunction(DatabaseInstance &instance) {
    TableFunction random_string_function("random_string", {}, RandomStringExecute, RandomStringBind, RandomStringGlobalInit);
    random_string_function.named_parameters["length"] = LogicalType::UBIGINT;
    ExtensionUtil::RegisterFunction(instance, random_string_function);
}

} // namespace duckdb_faker