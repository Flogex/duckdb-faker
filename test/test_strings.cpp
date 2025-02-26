#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "faker_extension.hpp"

TEST_CASE("random_string", "[strings]") {
    duckdb::DuckDB db(nullptr);
    db.LoadStaticExtension<duckdb::FakerExtension>();
    duckdb::Connection con(db);

    SECTION("Should produce non-null strings") {
        const auto res = con.Query("FROM random_string() LIMIT 1");
        const auto val = res->GetValue(0, 0);
        REQUIRE(val.type() == duckdb::LogicalType::VARCHAR);
        REQUIRE_FALSE(val.IsNull());
    }
}