#pragma once

namespace duckdb {
class DatabaseInstance;
}

namespace duckdb_faker {

constexpr uint64_t DEFAULT_MAX_GENERATED_ROWS = 2 << 16;

struct RandomIntFunction {
	static void RegisterFunction(duckdb::DatabaseInstance &instance);
};

} // namespace duckdb_faker
