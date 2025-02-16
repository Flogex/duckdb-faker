#pragma once

#include "duckdb/common/string_util.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace duckdb_faker {

class ProbabilityDistribution {
public:
    enum class Type : uint8_t {
        UNIFORM = 0
    };

    static std::optional<Type> FromString(const std::string &input) {
        if (duckdb::StringUtil::CIEquals(input, "uniform")) {
            return std::make_optional(Type::UNIFORM);
        }
        return std::nullopt;
    }
};

} // namespace duckdb_faker