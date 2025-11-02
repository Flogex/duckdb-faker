#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "catch2/matchers/catch_matchers_string.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers/database_fixture.hpp"

#include <cstdint>

using duckdb_faker::test_helpers::DatabaseFixture;
using Catch::Matchers::ContainsSubstring;

constexpr uint32_t LIMIT = 100;

static void sanity_check(const duckdb::unique_ptr<duckdb::MaterializedQueryResult>& res) {
    if (res->HasError()) {
        FAIL(res->GetError());
    }

    REQUIRE(res->RowCount() == LIMIT);
    REQUIRE(res->Collection().ColumnCount() == 1);
    REQUIRE(res->Collection().ChunkCount() == 1);
}

TEST_CASE_METHOD(DatabaseFixture, "random_int bounds checks", "[numbers][integers]") {
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
        auto [min, max] = GENERATE(std::make_tuple<int32_t, int32_t>(-1000, 42),
                                   std::make_tuple<int32_t, int32_t>(42, 1000),
                                   std::make_tuple<int32_t, int32_t>(-42, 42));

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
        auto [min, max] = GENERATE(std::make_tuple<int32_t, int32_t>(1, 0), std::make_tuple<int32_t, int32_t>(-5, -6));

        const auto query = std::format("SELECT value FROM random_int(min={}, max={})", min, max);
        auto res = con.Query(query);

        REQUIRE(res->HasError());
        CHECK_THAT(res->GetError(),
            ContainsSubstring("Invalid Input Error: Minimum value must be less than or equal to maximum value"));
    }
}

TEST_CASE_METHOD(DatabaseFixture, "random_int distributions", "[numbers][integers]") {
    auto test_uniform_distribution = [&](const std::string& distribution_param = "") {
        constexpr uint32_t min = 1;
        constexpr uint32_t max = 4;
        constexpr uint32_t limit = 10000;

        const auto distribution_clause =
            distribution_param.empty() ? "" : std::format(", distribution='{}'", distribution_param);
        const auto query = std::format("SELECT value, COUNT(1) FROM "
                                       "(FROM random_int(min={}, max={}{}) LIMIT {}) "
                                       "GROUP BY value "
                                       "ORDER BY value",
                                       min,
                                       max,
                                       distribution_clause,
                                       limit);
        const auto res = con.Query(query);

        const auto num_of_ones = res->GetValue(1, 0).GetValue<int64_t>();
        const auto num_of_twos = res->GetValue(1, 1).GetValue<int64_t>();
        const auto num_of_threes = res->GetValue(1, 2).GetValue<int64_t>();
        const auto num_of_fours = res->GetValue(1, 3).GetValue<int64_t>();
        // Some arbitrary bounds
        constexpr auto lower_bound = limit / 4 - limit * 0.025;
        constexpr auto upper_bound = limit / 4 + limit * 0.025;

        for (auto num_of_occurrences : {num_of_ones, num_of_twos, num_of_threes, num_of_fours}) {
            CHECK(num_of_occurrences >= lower_bound);
            CHECK(num_of_occurrences <= upper_bound);
        }
    };

    SECTION("Should produce uniform distribution by default", "[!mayfail]") {
        test_uniform_distribution();
    }

    SECTION("Should produce uniform distribution when explicitly specified", "[!mayfail]") {
        test_uniform_distribution("uniform");
    }

    SECTION("Should reject unknown distribution arguments") {
        auto res = con.Query("FROM random_int(distribution='unknown')");
        REQUIRE(res->HasError());
        CHECK_THAT(res->GetError(),
            ContainsSubstring("Invalid Input Error: Unknown probability distribution \"unknown\""));
    }
}