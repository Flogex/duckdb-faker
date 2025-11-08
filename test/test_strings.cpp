#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "catch2/matchers/catch_matchers_string.hpp"
#include "duckdb/common/types.hpp"
#include "test_helpers/database_fixture.hpp"

#include <cctype>
#include <cstdint>
#include <limits>
#include <map>
#include <string>

using Catch::Matchers::ContainsSubstring;
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

TEST_CASE_METHOD(DatabaseFixture, "random_string", "[strings]") {
    SECTION("Should produce non-null strings") {
        const auto res = con.Query("FROM random_string() LIMIT 1");
        const auto val = res->GetValue(0, 0);

        REQUIRE(val.type() == duckdb::LogicalType::VARCHAR);
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Should produce strings with only letters") {
        const auto query = std::format("FROM random_string() LIMIT {}", LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            for (char c : val) {
                CAPTURE(c);
                CHECK(std::isalpha(c));
            }
        }
    }
}

TEST_CASE_METHOD(DatabaseFixture, "random_string length", "[strings]") {
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
        const auto res = con.Query("FROM random_string(length='invalid')");
        REQUIRE(res->HasError());
        CHECK_THAT(res->GetError(), ContainsSubstring("Could not convert string 'invalid' to UINT64"));
    }

    SECTION("Should reject length arguments that are out of range") {
        const auto invalid_length = GENERATE("-1", "18446744073709551616" /* UINT64_MAX + 1 */);

        const auto query = std::format("FROM random_string(length={})", invalid_length);
        const auto res = con.Query(query);

        REQUIRE(res->HasError());
        CHECK_THAT(res->GetError(), ContainsSubstring("value is out of range"));
    }

    SECTION("Should throw error when length and min/max length are both specified") {
        const auto query = GENERATE("FROM random_string(length=10, min_length=1)",
                                    "FROM random_string(length=10, max_length=5)",
                                    "FROM random_string(length=10, min_length=1, max_length=5)");

        const auto res = con.Query(query);

        REQUIRE(res->HasError());
        REQUIRE_THAT(res->GetError(), ContainsSubstring("Can only specify either length or min_length/max_length"));
    }

    SECTION("Should produce strings with given minimum length") {
        const uint64_t min_length = GENERATE(10, 1000);

        const auto query = std::format("FROM random_string(min_length={}) LIMIT {}", min_length, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CHECK(val.size() >= min_length);
        }
    }

    SECTION("Should produce strings with given maximum length") {
        const uint64_t max_length = GENERATE(10, 1000);

        const auto query = std::format("FROM random_string(max_length={}) LIMIT {}", max_length, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CHECK(val.size() <= max_length);
        }
    }

    SECTION("Should produce strings with given minimum and maximum length") {
        auto [min_length, max_length] = GENERATE(std::make_tuple<uint64_t, uint64_t>(10, 20),
                                                 std::make_tuple<uint64_t, uint64_t>(100, 200),
                                                 std::make_tuple<uint64_t, uint64_t>(10, 10));

        const auto query =
            std::format("FROM random_string(min_length={}, max_length={}) LIMIT {}", min_length, max_length, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CHECK(val.size() >= min_length);
            CHECK(val.size() <= max_length);
        }
    }

    SECTION("Should throw error if min_length is greater than max_length") {
        const auto query = "FROM random_string(min_length=100, max_length=50)";
        const auto res = con.Query(query);
        REQUIRE(res->HasError());
        CHECK_THAT(res->GetError(), ContainsSubstring("min_length cannot be greater than max_length"));
    }

    SECTION("Should choose sane default for max_length if only min_length specified") {
        // For min_length < 10, max_length is 20.
        // For min_length >= 10, max_length should be min_length * 2.
        auto [min_length, max_length] = GENERATE(std::make_tuple<uint64_t, uint64_t>(1, 20),
                                                 std::make_tuple<uint64_t, uint64_t>(9, 20),
                                                 std::make_tuple<uint64_t, uint64_t>(10, 20),
                                                 std::make_tuple<uint64_t, uint64_t>(50, 100),
                                                 std::make_tuple<uint64_t, uint64_t>(100, 200),
                                                 std::make_tuple<uint64_t, uint64_t>(1000, 2000));

        const auto query =
            std::format("FROM random_string(min_length={}, max_length={}) LIMIT {}", min_length, max_length, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CHECK(val.size() >= min_length);
            CHECK(val.size() <= max_length);
        }
    }

    SECTION("Should produce strings of different lengths") {
        const uint64_t min_length = 10;
        const uint64_t max_length = 49;
        const uint32_t num_strings = 10000;

        const auto query = std::format(
            "FROM random_string(min_length={}, max_length={}) LIMIT {}", min_length, max_length, num_strings);
        const auto res = con.Query(query);

        std::map<uint64_t, uint32_t> length_counts;
        for (uint32_t row = 0; row < num_strings; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            const auto len = val.size();
            length_counts[len]++;
        }

        // We generated 10000 strings. We expect all 40 different lengths to be present.
        REQUIRE(length_counts.size() == 40);
    }
}

TEST_CASE_METHOD(DatabaseFixture, "random_string length slow", "[strings][.slow]") {
    SECTION("Should be able to produce strings with minimum_length UINT64_MAX") {
        const auto query = std::format("FROM random_string(min_length={}) LIMIT 1", UINT64_MAX);
        const auto res = con.Query(query);
        const auto val = res->GetValue(0, 0).GetValue<std::string>();
        REQUIRE(val.size() == UINT64_MAX);
    }

    SECTION("Should not overflow when calculating max_length for large min_length") {
        // max_length should not overflow
        auto [min_length, max_length] =
            GENERATE(std::make_tuple<uint64_t, uint64_t>(UINT64_MAX / 2, UINT64_MAX),
                     std::make_tuple<uint64_t, uint64_t>(UINT64_MAX / 2 + 5000, UINT64_MAX));

        const auto query =
            std::format("FROM random_string(min_length={}, max_length={}) LIMIT 3", min_length, max_length);
        const auto res = con.Query(query);

        for (uint32_t row = 0; row < res->RowCount(); row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CHECK(val.size() >= min_length);
            CHECK(val.size() <= max_length);
        }
    }
}

TEST_CASE_METHOD(DatabaseFixture, "random_string casing", "[strings]") {
    auto test_lower_casing = [&](const std::string& casing_param = "") {
        const auto casing_clause = casing_param.empty() ? "" : std::format("casing='{}'", casing_param);
        const auto query = std::format("FROM random_string({}) LIMIT {}", casing_clause, LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CAPTURE(val);
            for (const char c : val) {
                REQUIRE(std::islower(c));
            }
        }
    };

    SECTION("Should produce lowercase strings by default") {
        test_lower_casing();
    }

    SECTION("Should produce lowercase strings when specified") {
        test_lower_casing("lower");
    }

    SECTION("Should produce uppercase strings when specified") {
        const auto query = std::format("FROM random_string(casing='upper') LIMIT {}", LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            CAPTURE(val);
            for (char c : val) {
                REQUIRE(std::isupper(c));
            }
        }
    }

    SECTION("Should produce mixed case strings when specified") {
        const auto query = std::format("FROM random_string(length=250, casing='mixed') LIMIT {}", LIMIT);
        const auto res = con.Query(query);

        sanity_check(res);
        for (uint32_t row = 0; row < LIMIT; row++) {
            const auto val = res->GetValue(0, row).GetValue<std::string>();
            bool has_upper = false;
            bool has_lower = false;
            for (char c : val) {
                if (std::isupper(c))
                    has_upper = true;
                if (std::islower(c))
                    has_lower = true;
            }
            CAPTURE(val);
            // For a string of length 250, we expect to have at least one upper and one lower case letter
            CHECK(has_upper);
            CHECK(has_lower);
        }
    }

    SECTION("Should reject invalid casing values") {
        const auto query = "FROM random_string(casing='invalid')";
        const auto res = con.Query(query);

        REQUIRE(res->HasError());
        CHECK_THAT(res->GetError(), ContainsSubstring("casing must be one of: lower, upper, mixed"));
    }
}