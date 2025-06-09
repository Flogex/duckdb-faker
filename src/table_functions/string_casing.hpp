#include <optional>
#include <stdexcept>

namespace duckdb_faker {
enum class StringCasing {
    Lower,
    Upper,
    Mixed
};

std::optional<StringCasing> string_casing_from_string(const std::string& casing) {
    if (casing == "lower") {
        return StringCasing::Lower;
    } else if (casing == "upper") {
        return StringCasing::Upper;
    } else if (casing == "mixed") {
        return StringCasing::Mixed;
    } else {
        return std::nullopt;
    }
}

faker::string::StringCasing to_faker_casing(StringCasing casing) {
    switch (casing) {
    case StringCasing::Lower:
        return faker::string::StringCasing::Lower;
    case StringCasing::Upper:
        return faker::string::StringCasing::Upper;
    case StringCasing::Mixed:
        return faker::string::StringCasing::Mixed;
    }
    // Should never happen
    throw std::runtime_error("Invalid string casing");
}

} // namespace duckdb_faker