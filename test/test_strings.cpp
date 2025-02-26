#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "duckdb/common/types.hpp"
#include "test_helpers/database_fixture.hpp"

using namespace duckdb_faker::test_helpers;

TEST_CASE_METHOD(DatabaseFixture, "random_string", "[strings]") {
    SECTION("Should produce non-null strings") {
        const auto res = con.Query("FROM random_string() LIMIT 1");
        const auto val = res->GetValue(0, 0);
        REQUIRE(val.type() == duckdb::LogicalType::VARCHAR);
        REQUIRE_FALSE(val.IsNull());
    }
}