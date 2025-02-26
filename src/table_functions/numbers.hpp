#pragma once

#include "utils/database_instance.hpp"

namespace duckdb_faker {

struct RandomIntFunction {
	static void RegisterFunction(duckdb::DatabaseInstance &instance);
};

} // namespace duckdb_faker
