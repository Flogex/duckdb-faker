#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "catch2/matchers/catch_matchers.hpp"
#include "catch2/matchers/catch_matchers_string.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "faker_extension.hpp"
#include "test_helpers/database_fixture.hpp"

#include <cstdint>

using duckdb_faker::test_helpers::DatabaseFixture;

constexpr uint32_t LIMIT = 100;

static void sanity_check(const duckdb::unique_ptr<duckdb::MaterializedQueryResult>& res) {
    if (res->HasError()) {
        FAIL(res->GetError());
    }

    REQUIRE(res->RowCount() == LIMIT);
    REQUIRE(res->Collection().ColumnCount() == 1);
    REQUIRE(res->Collection().ChunkCount() == 1);
}

TEST_CASE_METHOD(DatabaseFixture, "random_bool", "[booleans]") {
    SECTION("Should generate true and false values with equal probability by default") {
        constexpr uint32_t limit = 100'000;
        const auto query = std::format("WITH random_bools AS (FROM random_bool() LIMIT {}) "
                                       "SELECT value, COUNT(value) FROM random_bools "
                                       "GROUP BY value ORDER BY value",
                                       limit);
        const auto res = con.Query(query);

        REQUIRE(res->GetValue<bool>(0, 0) == false);
        const auto false_count = res->GetValue<uint64_t>(1, 0);
        REQUIRE(res->GetValue<bool>(0, 1) == true);
        const auto true_count = res->GetValue<uint64_t>(1, 1);
        CAPTURE(true_count, false_count);
        REQUIRE(true_count + false_count == limit);

        // We expect a difference of 1% or less.
        //  true_count + false_count == 100'000, hence expected count is 50'000.
        CHECK((true_count >= 49'000 && true_count <= 51'000));
    }

    SECTION("Should only generate \"false\" if true_probability is 0") {
        const auto query = std::format("FROM random_bool(true_probability=0.0) LIMIT {}", LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        duckdb::Vector data = res->Fetch()->data[0];
        for (uint32_t i = 0; i < LIMIT; i++) {
            CHECK(data.GetValue(i).GetValue<bool>() == false);
        }
    }

    SECTION("Should only generate \"true\" if true_probability is 1") {
        const auto query = std::format("FROM random_bool(true_probability=1.0) LIMIT {}", LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        duckdb::Vector data = res->Fetch()->data[0];
        for (uint32_t i = 0; i < LIMIT; i++) {
            CHECK(data.GetValue(i).GetValue<bool>() == true);
        }
    }

    SECTION("Should reject true_probability outside of [0, 1]") {
        // std::format thinks -0.001 is equal to 0
        const double true_probability_times_1000 = GENERATE(-2000, -1, 1001, 2000);

        const auto query =
            std::format("FROM random_bool(true_probability={}/1000) LIMIT {}", true_probability_times_1000, LIMIT);
        const auto res = con.Query(query);

        REQUIRE(res->HasError());
        REQUIRE_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("true_probability must be between 0 and 1"));
    }

    SECTION("Should generate true and false values with given true_probability") {
        const double true_probability = GENERATE(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9);
        CAPTURE(true_probability);

        constexpr uint32_t row_count = 100'000;
        const auto query = std::format("WITH random_bools AS (SELECT value "
                                       "FROM random_bool(true_probability={}) LIMIT {}) "
                                       "SELECT value, COUNT(value) FROM random_bools "
                                       "GROUP BY value ORDER BY value",
                                       true_probability,
                                       row_count);
        const auto res = con.Query(query);

        REQUIRE(res->GetValue<bool>(0, 0) == false);
        const auto false_count = res->GetValue<uint64_t>(1, 0);
        REQUIRE(res->GetValue<bool>(0, 1) == true);
        const auto true_count = res->GetValue<uint64_t>(1, 1);
        CAPTURE(true_count, false_count);
        REQUIRE(true_count + false_count == row_count);

        // We expect a difference of 1% or less.
        const auto max_difference = (uint32_t)(row_count * 0.01);
        const auto expected_min_true_count = ((uint32_t)(row_count * true_probability)) - max_difference;
        const auto expected_max_true_count = ((uint32_t)(row_count * true_probability)) + max_difference;

        CAPTURE(true_probability);
        CAPTURE(expected_min_true_count, expected_max_true_count);

        CHECK((true_count >= expected_min_true_count && true_count <= expected_max_true_count));
    }
}