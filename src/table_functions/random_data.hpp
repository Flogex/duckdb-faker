#pragma once

#include "utils/extension_loader_decl.hpp"

namespace duckdb_faker {

struct RandomDataFunction {
    static void RegisterFunction(duckdb::ExtensionLoader& loader);
};

} // namespace duckdb_faker
