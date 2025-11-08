#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

#include <cstdint>

namespace duckdb {
class ClientContext;
class DataChunk;
struct FunctionData;
class optional_idx;
} // namespace duckdb

namespace duckdb_faker::rowid_generator {

duckdb::virtual_column_map_t GetVirtualColumns(duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData>);
duckdb::vector<duckdb::column_t> GetRowIdColumns(duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData>);

void PopulateRowIdColumn(uint64_t start_rowid, duckdb::optional_idx rowid_column_idx, duckdb::DataChunk& output);

} // namespace duckdb_faker::rowid_generator