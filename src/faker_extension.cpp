#define DUCKDB_EXTENSION_MAIN

#include "faker_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "table_functions/booleans.hpp"
#include "table_functions/numbers.hpp"
#include "table_functions/strings.hpp"

namespace duckdb {

void FakerExtension::LoadInternal(ExtensionLoader& loader) {
    duckdb_faker::RandomBoolFunction::RegisterFunction(loader);
    duckdb_faker::RandomIntFunction::RegisterFunction(loader);
    duckdb_faker::RandomStringFunction::RegisterFunction(loader);
}

void FakerExtension::Load(ExtensionLoader& loader) {
    LoadInternal(loader);
}

std::string FakerExtension::Name() {
    return "faker";
}

std::string FakerExtension::Version() const {
    return "0.1";
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(faker, ext_loader) {
    duckdb::FakerExtension::LoadInternal(ext_loader);
}
}