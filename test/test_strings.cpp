#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "catch2/matchers/catch_matchers_string.hpp"
#include "duckdb/common/types.hpp"
#include "test_helpers/database_fixture.hpp"

#include <cstdint>
#include <string>

using namespace duckdb_faker::test_helpers;

constexpr uint32_t LIMIT = 100;

static void sanity_check(const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &res) {
    if (res->HasError()) {
        FAIL(res->GetError());
    }

    REQUIRE(res->RowCount() == LIMIT);
    REQUIRE(res->Collection().ColumnCount() == 1);
    REQUIRE(res->Collection().ChunkCount() == 1);
}

TEST_CASE_METHOD(DatabaseFixture, "random_string", "[strings]") {
    SECTION("Should produce non-null strings") {
        const auto res = con.Query("FROM random_string() LIMIT 1");
        const auto val = res->GetValue(0, 0);

        REQUIRE(val.type() == duckdb::LogicalType::VARCHAR);
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Should produce strings of the specified length") {
        const uint32_t length = GENERATE(1, 10, 100, 1000);

        const auto query = std::format("FROM random_string(length={}) LIMIT {}", length, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);

        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            REQUIRE(val.size() == length);
        }
    }

    SECTION("Should reject invalid length arguments") {
        const auto invalid_length = GENERATE("-1", "18446744073709551616" /* UINT64_MAX + 1 */);

        const auto query = std::format("FROM random_string(length={})", invalid_length);
        const auto res = con.Query(query);

        CHECK(res->HasError());
        CHECK_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("value is out of range"));
    }
}