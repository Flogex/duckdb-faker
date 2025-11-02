#include "rowid_generator.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "utils/client_context_decl.hpp"

#include <cstdint>
#include <limits>

using namespace duckdb;

namespace duckdb_faker::rowid_generator {

virtual_column_map_t GetVirtualColumns(ClientContext&, optional_ptr<FunctionData>) {
    virtual_column_map_t virtual_columns;
    virtual_columns.emplace(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE));
    return virtual_columns;
}

vector<column_t> GetRowIdColumns(ClientContext&, optional_ptr<FunctionData>) {
    return {COLUMN_IDENTIFIER_ROW_ID};
}

void PopulateRowIdColumn(const uint64_t start_rowid, const optional_idx rowid_column_idx, DataChunk& output) {
    D_ASSERT(rowid_column_idx.IsValid());

    const idx_t cardinality = output.size();

    Vector& rowid_vector = output.data[rowid_column_idx.GetIndex()];
    D_ASSERT(rowid_vector.GetType().id() == LogicalType::ROW_TYPE);
    D_ASSERT(rowid_vector.GetVectorType() == VectorType::FLAT_VECTOR);

    if (cardinality > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - start_rowid) {
        throw InvalidInputException("Row ID overflow: cannot generate row IDs beyond INT64_MAX");
    }

    auto rowid_data = FlatVector::GetData<int64_t>(rowid_vector);
    for (idx_t relative_rowid = 0; relative_rowid < cardinality; relative_rowid++) {
        rowid_data[relative_rowid] = static_cast<int64_t>(start_rowid + relative_rowid);
    }
    // No need to modify the validity mask because rowid is never NULL
}

} // namespace duckdb_faker::shared_generator
