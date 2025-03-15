#pragma once

#include "duckdb/function/table_function.hpp"

#include <cstdint>

namespace duckdb_faker {

struct GeneratorGlobalState : duckdb::GlobalTableFunctionState {
    static constexpr uint64_t DEFAULT_MAX_GENERATED_ROWS = STANDARD_VECTOR_SIZE * 64;

    uint64_t num_generated_rows = 0;
    uint64_t max_generated_rows = DEFAULT_MAX_GENERATED_ROWS;
};

} // namespace duckdb_faker