#include <duckdb/main/extension_helper.hpp>
#include "faker_extension.hpp"
#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

constexpr uint32_t LIMIT = 100;

TEST_CASE("random_int", "[numbers]") {
    duckdb::DuckDB db(nullptr);
    db.LoadStaticExtension<duckdb::FakerExtension>();
    duckdb::Connection con(db);


    SECTION("Should only produce numbers greater or equal to minimum") {
        const int32_t min = GENERATE(-100, 0, 100);

        const auto query = std::format("SELECT value FROM random_int(min = {}) LIMIT {}", min, LIMIT);
        const auto res = con.Query(query);

        REQUIRE(res->RowCount() == LIMIT);
        REQUIRE(res->Collection().ColumnCount() == 1);
        REQUIRE(res->Collection().ChunkCount() == 1);

        duckdb::Vector data = res->Fetch()->data[0];
        for (uint32_t i = 0; i < LIMIT; i++) {
            CHECK(data.GetValue(i).GetValue<int32_t>() >= min);
        }

    }
}