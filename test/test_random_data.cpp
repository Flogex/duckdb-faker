#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "catch2/matchers/catch_matchers_string.hpp"
#include "test_helpers/database_fixture.hpp"

#include <cstdint>
#include <sstream>
#include <string>

using duckdb_faker::test_helpers::DatabaseFixture;

TEST_CASE_METHOD(DatabaseFixture, "random_data", "[mixed_types]") {
    SECTION("Should raise error when schema_source not specified") {
        const auto res = con.Query("FROM random_data()");
        REQUIRE(res->HasError());
        REQUIRE_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("Missing required named parameter"));
    }
}

// TODO: Add test that checks that default parameters are used for generators
TEST_CASE_METHOD(DatabaseFixture, "random_data source_schema", "[mixed_types]") {
    SECTION("Should raise error when schema_source table does not exist") {
        const auto res = con.Query("FROM random_data(schema_source='non_existent_table')");
        REQUIRE(res->HasError());
        REQUIRE_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("does not exist"));
    }

    SECTION("Should produce columns matching the source schema") {
        con.Query("CREATE TABLE source_tbl (a INT, b BOOLEAN, c VARCHAR)");

        const auto res = con.Query("SELECT a, b, c FROM random_data(schema_source='source_tbl') LIMIT 1");
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        const auto column_types = res->Collection().Types();
        CAPTURE(column_types);
        CHECK(column_types[0] == duckdb::LogicalType::INTEGER);
        CHECK(column_types[1] == duckdb::LogicalType::BOOLEAN);
        CHECK(column_types[2] == duckdb::LogicalType::VARCHAR);
    }

    SECTION("Recognized fully-qualified table name") {
        con.Query("ATTACH ':memory:' AS test_db");
        con.Query("CREATE SCHEMA test_db.test_schema");
        con.Query("CREATE TABLE test_db.test_schema.fq_tbl1 (a INT)");
        con.Query("CREATE TABLE test_db.main.fq_tbl2 (a INT)");
        con.Query("USE test_db");

        {
            const auto res1 = con.Query("FROM random_data(schema_source='test_schema.fq_tbl1') LIMIT 1");
            CHECK_FALSE(res1->HasError());
        }
        {
            const auto res2 = con.Query("FROM random_data(schema_source='test_db.test_schema.fq_tbl1') LIMIT 1");
            CHECK_FALSE(res2->HasError());
        }
        {
            const auto res3 = con.Query("FROM random_data(schema_source='test_db.fq_tbl2') LIMIT 1");
            CHECK_FALSE(res3->HasError());
        }
        {
            const auto res4 = con.Query("FROM random_data(schema_source='test_db.main.fq_tbl2') LIMIT 1");
            CHECK_FALSE(res4->HasError());
        }
    }

    SECTION("Produces number of rows as per LIMIT") {
        con.Query("CREATE TABLE source_tbl (a INT, b BOOLEAN)");

        const auto res = con.Query("FROM random_data(schema_source='source_tbl') LIMIT 42");
        REQUIRE(res->RowCount() == 42);
    }

    SECTION("Can be used to insert into an existing table") {
        con.Query("CREATE TABLE my_tbl (a INT, b BOOLEAN)");
        const std::string query = "INSERT INTO my_tbl (b, a) "
                                  "SELECT b, a "
                                  "FROM random_data(schema_source='my_tbl') "
                                  "LIMIT 10";
        const auto insert_res = con.Query(query);
        REQUIRE_FALSE(insert_res->HasError());

        const auto select_res = con.Query("SELECT * FROM my_tbl");
        REQUIRE(select_res->RowCount() == 10);
    }
}

TEST_CASE_METHOD(DatabaseFixture, "random_data source_schema slow", "[mixed_types][.slow]") {
    SECTION("Should handle wide table gracefully", "[.failing]") {
        con.Query("SET max_expression_depth=20000");

        // When setting this to 10000, it segfaults during binding some 250 stack frames deep
        constexpr uint32_t num_columns = 999;
        std::ostringstream create_table_query;
        create_table_query << "CREATE TABLE wide_tbl (";
        for (uint32_t i = 0; i < num_columns; i++) {
            std::string type;
            if (i % 3 == 0) {
                type = "INT";
            } else if (i % 3 == 1) {
                type = "BOOLEAN";
            } else {
                type = "VARCHAR";
            }

            create_table_query << "col" << i << " " << type << ", ";
        }
        create_table_query << ")";
        con.Query(create_table_query.str());

        const auto res = con.Query("FROM random_data(schema_source='wide_tbl') LIMIT 10");
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == num_columns);
        const auto column_types = res->Collection().Types();
        // Do some spot checks
        CHECK(column_types[333] == duckdb::LogicalType::INTEGER);
        CHECK(column_types[3001] == duckdb::LogicalType::BOOLEAN);
        CHECK(column_types[6002] == duckdb::LogicalType::VARCHAR);
    }
}
