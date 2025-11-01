#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/function/table_function.hpp"

#include <cstdint>

namespace duckdb_faker {

// The generator table functions only have two columns:
// The 'value' column containing the actual result and the virtual 'rowid' column.
// If any of the columns is not projected, the index will be invalid.
struct GeneratorColumnIndexes {
    duckdb::optional_idx rowid_idx;
    duckdb::optional_idx value_idx;
};

struct GeneratorGlobalState : duckdb::GlobalTableFunctionState {
    static constexpr uint64_t DEFAULT_MAX_GENERATED_ROWS = STANDARD_VECTOR_SIZE * 64;

    explicit GeneratorGlobalState(const duckdb::TableFunctionInitInput& input);

    uint64_t num_generated_rows = 0;
    uint64_t max_generated_rows = DEFAULT_MAX_GENERATED_ROWS;
    GeneratorColumnIndexes column_indexes;
};

} // namespace duckdb_faker