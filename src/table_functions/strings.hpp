#pragma once

#include "utils/database_instance.hpp"

namespace duckdb_faker {

struct RandomStringFunction {
    static void RegisterFunction(duckdb::DatabaseInstance &instance);
};

} // namespace duckdb_faker