#pragma once

#include "duckdb/function/table_function.hpp"

#include <cstdint>

namespace duckdb_faker {

constexpr uint64_t DEFAULT_MAX_GENERATED_ROWS = 2 << 16;

struct GeneratorGlobalState : duckdb::GlobalTableFunctionState {
    uint64_t num_generated_rows = 0;
    uint64_t max_generated_rows = DEFAULT_MAX_GENERATED_ROWS;
};

} // namespace duckdb_faker