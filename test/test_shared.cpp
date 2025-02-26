#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "faker_extension.hpp"

// Currently we cut of at a maximum cardinality of 2^16
TEST_CASE("Should produce the number of rows specified by LIMIT", "[shared]") {
    duckdb::DuckDB db(nullptr);
    db.LoadStaticExtension<duckdb::FakerExtension>();
    duckdb::Connection con(db);

    const int32_t limit = GENERATE(0, 10, 100, 100000);
    const std::string table_function = GENERATE("random_int", "random_string");
    const auto query = std::format("FROM {}() LIMIT {}", table_function, limit);
    const auto res = con.Query(query);

    REQUIRE(res->RowCount() == limit);
}