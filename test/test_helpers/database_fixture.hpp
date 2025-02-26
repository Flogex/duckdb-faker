#pragma once

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "faker_extension.hpp"

namespace duckdb_faker::test_helpers {

class DatabaseFixture {
public:
    DatabaseFixture() : db(nullptr), con(db) {
        db.LoadStaticExtension<duckdb::FakerExtension>();
    }

    duckdb::DuckDB db;
    duckdb::Connection con;
};

} // namespace duckdb_faker::test_helpers