#include "generator_global_state.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/table_function.hpp"

using namespace duckdb;

namespace duckdb_faker {

namespace {
GeneratorColumnIndexes get_column_indexes(const TableFunctionInitInput& input) {
    GeneratorColumnIndexes column_indexes;

    // There are only two columns: the 'value' column and the virtual 'rowid' column
    // input.column_indexes contains the indices of columns that are projected or filtered on.
    for (idx_t p = 0; p < input.column_indexes.size(); p++) {
        if (input.column_indexes[p].IsRowIdColumn()) {
            D_ASSERT(!column_indexes.rowid_idx.IsValid()); // There should only be one 'rowid' column
            column_indexes.rowid_idx = p;
        } else {
            D_ASSERT(!column_indexes.value_idx.IsValid()); // There should only be one 'value' column
            column_indexes.value_idx = p;
        }
    }

    // At least one of the columns should be projected
    D_ASSERT(column_indexes.value_idx.IsValid() ||column_indexes.rowid_idx.IsValid());

    return column_indexes;
}
}

GeneratorGlobalState::GeneratorGlobalState(const TableFunctionInitInput& input) {
    column_indexes = get_column_indexes(input);
}

} // namespace duckdb_faker