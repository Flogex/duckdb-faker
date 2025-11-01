#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "faker_extension.hpp"
#include "test_helpers/database_fixture.hpp"

using duckdb_faker::test_helpers::DatabaseFixture;

TEST_CASE_METHOD(DatabaseFixture, "Generator functions expose 'rowid' column", "[rowid]") {
    const std::string table_function = GENERATE("random_bool", "random_int", "random_string");
    CAPTURE(table_function);

    SECTION("rowid column is present and with correct values") {
        const auto query = std::format("SELECT rowid FROM {}() LIMIT 10", table_function);
        const auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->RowCount() == 10);
        REQUIRE(res->ColumnCount() == 1);

        for (idx_t row_id = 0; row_id < 10; row_id++) {
            REQUIRE(res->GetValue(0, row_id).GetValue<int64_t>() == row_id);
        }
    }

    SECTION("rowid and value columns can be selected together") {
        {
            const auto query1 = std::format("SELECT rowid, value FROM {}() LIMIT 10", table_function);
            const auto res1 = con.Query(query1);
            REQUIRE_FALSE(res1->HasError());
            REQUIRE(res1->RowCount() == 10);
            REQUIRE(res1->ColumnCount() == 2);
            // Spotcheck at row index 5
            REQUIRE(res1->GetValue(0, 5).GetValue<int64_t>() == 5);
            REQUIRE_FALSE(res1->GetValue(1, 5).IsNull());
        }
        {
            const auto query2 = std::format("SELECT value, rowid FROM {}() LIMIT 10", table_function);
            const auto res2 = con.Query(query2);
            REQUIRE_FALSE(res2->HasError());
            REQUIRE(res2->RowCount() == 10);
            REQUIRE(res2->ColumnCount() == 2);
            REQUIRE(res2->GetValue(1, 5).GetValue<int64_t>() == 5);
            REQUIRE_FALSE(res2->GetValue(0, 5).IsNull());
        }
        {
            const auto query3 = std::format("SELECT rowid, * FROM {}() LIMIT 10", table_function);
            const auto res3 = con.Query(query3);
            REQUIRE_FALSE(res3->HasError());
            REQUIRE(res3->RowCount() == 10);
            REQUIRE(res3->ColumnCount() == 2);
            REQUIRE(res3->GetValue(0, 5).GetValue<int64_t>() == 5);
            REQUIRE_FALSE(res3->GetValue(1, 5).IsNull());
        }
    }

    SECTION("rowid can be used in WHERE clause") {
        {
            const auto query1 = std::format("SELECT rowid, value FROM {}() WHERE rowid = 42", table_function);
            const auto res1 = con.Query(query1);
            REQUIRE_FALSE(res1->HasError());
            REQUIRE(res1->RowCount() == 1);
            REQUIRE(res1->ColumnCount() == 2);
            REQUIRE(res1->GetValue(0, 0).GetValue<int64_t>() == 42);
            REQUIRE_FALSE(res1->GetValue(1, 0).IsNull());
        }
        {
            const auto query2 = std::format("SELECT value FROM {}() WHERE rowid = 42", table_function);
            const auto res2 = con.Query(query2);
            REQUIRE_FALSE(res2->HasError());
            REQUIRE(res2->RowCount() == 1);
            REQUIRE(res2->ColumnCount() == 1);
            REQUIRE_FALSE(res2->GetValue(0, 0).IsNull());
        }
    }

    SECTION("rowid is correct in presence of OFFSET") {
        const auto query = std::format("SELECT rowid FROM {}() OFFSET 100 LIMIT 10", table_function);
        const auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->RowCount() == 10);
        REQUIRE(res->ColumnCount() == 1);

        for (idx_t result_id = 0; result_id < 10; result_id++) {
            REQUIRE(res->GetValue(0, result_id).GetValue<int64_t>() == result_id + 100);
        }
    }
}