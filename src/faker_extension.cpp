#define DUCKDB_EXTENSION_MAIN

#include "faker_extension.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"
#include "table_functions/numbers.hpp"

namespace duckdb {

void FakerExtension::Load(DuckDB &db) {
    DatabaseInstance &instance = *db.instance;
    duckdb_faker::RandomIntFunction::RegisterFunction(instance);
}

std::string FakerExtension::Name() {
    return "faker";
}

std::string FakerExtension::Version() const {
    return "0.1";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void faker_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::FakerExtension>();
}

DUCKDB_EXTENSION_API const char *faker_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
