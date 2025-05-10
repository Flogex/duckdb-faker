#pragma once

#include "duckdb/main/extension.hpp"
#include <string>

namespace duckdb {

class FakerExtension final : public Extension {
public:
    void Load(DuckDB& db) override;
    std::string Name() override;
    std::string Version() const override;
};

} // namespace duckdb
