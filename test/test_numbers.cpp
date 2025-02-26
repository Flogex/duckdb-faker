#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "faker_extension.hpp"

#include <cstdint>

constexpr uint32_t LIMIT = 100;

static void sanity_check(const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &res) {
    REQUIRE(res->RowCount() == LIMIT);
    REQUIRE(res->Collection().ColumnCount() == 1);
    REQUIRE(res->Collection().ChunkCount() == 1);
}

TEST_CASE("random_int", "[numbers]") {
    duckdb::DuckDB db(nullptr);
    db.LoadStaticExtension<duckdb::FakerExtension>();
    duckdb::Connection con(db);

    SECTION("Should only produce numbers greater or equal to minimum") {
        const int32_t min = GENERATE(-100, 0, 100);

        const auto query = std::format("SELECT value FROM random_int(min={}) LIMIT {}", min, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        duckdb::Vector data = res->Fetch()->data[0];
        for (uint32_t i = 0; i < LIMIT; i++) {
            CHECK(data.GetValue(i).GetValue<int32_t>() >= min);
        }
    }

    SECTION("Should only produce numbers less or equal to maximum") {
        const int32_t max = GENERATE(-100, 0, 100);

        const auto query = std::format("SELECT value FROM random_int(max={}) LIMIT {}", max, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        duckdb::Vector data = res->Fetch()->data[0];
        for (uint32_t i = 0; i < LIMIT; i++) {
            CHECK(data.GetValue(i).GetValue<int32_t>() <= max);
        }
    }

    SECTION("Should respect both minimum and maximum") {
        auto [min, max] = GENERATE(
            std::make_tuple<int32_t, int32_t>(-1000, 42),
            std::make_tuple<int32_t, int32_t>(42, 1000),
            std::make_tuple<int32_t, int32_t>(-42, 42)
        );

        const auto query = std::format("SELECT value FROM random_int(min={}, max={}) LIMIT {}", min, max, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        duckdb::Vector data = res->Fetch()->data[0];
        for (uint32_t i = 0; i < LIMIT; i++) {
            const auto val = data.GetValue(i).GetValue<int32_t>();
            CHECK(val >= min);
            CHECK(val <= max);
        }
    }

    SECTION("Should reject a minimum greater than maximum") {
        auto [min, max] = GENERATE(
            std::make_tuple<int32_t, int32_t>(1, 0),
            std::make_tuple<int32_t, int32_t>(-5, -6)
        );

        const auto query = std::format("SELECT value FROM random_int(min={}, max={})", min, max);
        auto res = con.Query(query);

        REQUIRE(res->HasError());
        REQUIRE(res->GetError() == "Invalid Input Error: Minimum value must be less than or equal to maximum value");
    }

    SECTION("Should produce uniform distribution by default", "[!mayfail]") {
        const auto min = 1;
        const auto max = 4;
        const auto LIMIT = 10000;

        const auto query = std::format("SELECT value, COUNT(1) FROM "
                                       "(FROM random_int(min={}, max={}) LIMIT {}) "
                                       "GROUP BY value "
                                       "ORDER BY value",
                                       min, max, LIMIT);
        const auto res = con.Query(query);

        const auto num_of_ones = res->GetValue(1, 0).GetValue<int64_t>();
        const auto num_of_twos = res->GetValue(1, 1).GetValue<int64_t>();
        const auto num_of_threes = res->GetValue(1, 2).GetValue<int64_t>();
        const auto num_of_fours = res->GetValue(1, 3).GetValue<int64_t>();
        // Some arbitrary bounds
        const auto lower_bound = LIMIT / 4 - LIMIT * 0.025;
        const auto upper_bound = LIMIT / 4 + LIMIT * 0.025;

        for (auto num_of_occurrences : {num_of_ones, num_of_twos, num_of_threes, num_of_fours}) {
            CHECK(num_of_occurrences >= lower_bound);
            CHECK(num_of_occurrences <= upper_bound);
        }
    }

    SECTION("Should reject unknown distribution arguments") {
        auto res = con.Query("FROM random_int(distribution='unknown')");
        REQUIRE(res->HasError());
        REQUIRE(res->GetError() == "Invalid Input Error: Unknown probability distribution \"unknown\"");
    }
}