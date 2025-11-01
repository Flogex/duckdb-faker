#pragma once

#include "utils/extension_loader_decl.hpp"

namespace duckdb_faker {

struct RandomIntFunction {
    static void RegisterFunction(duckdb::ExtensionLoader& loader);
};

} // namespace duckdb_faker
