#include "catch2/catch_test_macros.hpp"
#include "src/table_functions/probability_distributions.hpp"
#include "src/table_functions/string_casing.hpp"

using namespace duckdb_faker;

TEST_CASE("ProbabilityDistribution::FromString", "[utilities]") {
    SECTION("Should recognize uniform distribution") {
        auto result = ProbabilityDistribution::FromString("uniform");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == ProbabilityDistribution::Type::UNIFORM);
    }

    SECTION("Should be case insensitive") {
        auto result1 = ProbabilityDistribution::FromString("UNIFORM");
        auto result2 = ProbabilityDistribution::FromString("Uniform");
        auto result3 = ProbabilityDistribution::FromString("UnIfOrM");

        REQUIRE(result1.has_value());
        REQUIRE(result2.has_value());
        REQUIRE(result3.has_value());
        REQUIRE(result1.value() == ProbabilityDistribution::Type::UNIFORM);
        REQUIRE(result2.value() == ProbabilityDistribution::Type::UNIFORM);
        REQUIRE(result3.value() == ProbabilityDistribution::Type::UNIFORM);
    }

    SECTION("Should return nullopt for unknown distributions") {
        auto result1 = ProbabilityDistribution::FromString("normal");
        auto result2 = ProbabilityDistribution::FromString("gaussian");
        auto result3 = ProbabilityDistribution::FromString("invalid");
        auto result4 = ProbabilityDistribution::FromString("");

        REQUIRE_FALSE(result1.has_value());
        REQUIRE_FALSE(result2.has_value());
        REQUIRE_FALSE(result3.has_value());
        REQUIRE_FALSE(result4.has_value());
    }
}

TEST_CASE("string_casing_from_string", "[utilities]") {
    SECTION("Should recognize valid casing options") {
        auto lower = string_casing_from_string("lower");
        auto upper = string_casing_from_string("upper");
        auto mixed = string_casing_from_string("mixed");

        REQUIRE(lower.has_value());
        REQUIRE(upper.has_value());
        REQUIRE(mixed.has_value());
        REQUIRE(lower.value() == StringCasing::Lower);
        REQUIRE(upper.value() == StringCasing::Upper);
        REQUIRE(mixed.value() == StringCasing::Mixed);
    }

    SECTION("Should be case sensitive") {
        auto result1 = string_casing_from_string("LOWER");
        auto result2 = string_casing_from_string("Lower");
        auto result3 = string_casing_from_string("UPPER");

        REQUIRE_FALSE(result1.has_value());
        REQUIRE_FALSE(result2.has_value());
        REQUIRE_FALSE(result3.has_value());
    }

    SECTION("Should return nullopt for unknown casing values") {
        auto result1 = string_casing_from_string("invalid");
        auto result2 = string_casing_from_string("title");
        auto result3 = string_casing_from_string("");

        REQUIRE_FALSE(result1.has_value());
        REQUIRE_FALSE(result2.has_value());
        REQUIRE_FALSE(result3.has_value());
    }
}

TEST_CASE("to_faker_casing", "[utilities]") {
    SECTION("Should correctly convert StringCasing to faker casing") {
        REQUIRE(to_faker_casing(StringCasing::Lower) == faker::string::StringCasing::Lower);
        REQUIRE(to_faker_casing(StringCasing::Upper) == faker::string::StringCasing::Upper);
        REQUIRE(to_faker_casing(StringCasing::Mixed) == faker::string::StringCasing::Mixed);
    }
}