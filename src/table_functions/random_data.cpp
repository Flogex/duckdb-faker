#include "random_data.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

#include <sstream>
#include <string>

using namespace duckdb;

namespace duckdb_faker {

namespace {
std::string logical_type_to_generator_name(const LogicalType& type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
        return "random_bool";
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
        return "random_int";
    case LogicalTypeId::VARCHAR:
        return "random_string";
    default:
        throw NotImplementedException("Random data generation not implemented for type: %s", type.ToString());
    }
}

unique_ptr<TableRef> RandomDataBindReplace(ClientContext& context, TableFunctionBindInput& input) {
    const auto schema_source_it = input.named_parameters.find("schema_source");
    if (schema_source_it == input.named_parameters.cend()) {
        throw InvalidInputException("Missing required named parameter: schema_source");
    }
    const std::string schema_source = schema_source_it->second.GetValue<string>();

    auto [catalog, schema, entry_name] = QualifiedName::Parse(schema_source);
    Binder::BindSchemaOrCatalog(context, catalog, schema);
    // TODO: Can we also have views as source?
    // This throws if the entry is not found
    CatalogEntry& entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, catalog, schema, entry_name);

    D_ASSERT(entry.type == TableCatalogEntry::Type);
    const auto& table_entry = entry.Cast<TableCatalogEntry>();

    // TODO: What if the table has generated columns?
    if (table_entry.HasGeneratedColumns()) {
        throw NotImplementedException("Tables with generated columns are not supported as schema_source yet");
    }
    // TODO: What about constraints?
    if (table_entry.GetConstraints().size() > 0) {
        throw NotImplementedException("Tables with constraints are not supported as schema_source yet");
    }

    duckdb::vector<ColumnDefinition> produced_columns;
    for (const auto& col : table_entry.GetColumns().Physical()) {
        // TODO: What if the column has default values?
        if (col.HasDefaultValue()) {
            throw NotImplementedException("Tables with default values are not supported as schema_source yet");
        }
        produced_columns.push_back(ColumnDefinition(col.Name(), col.Type()));
    }

    std::ostringstream subquery;
    // SELECT tf1.value AS my_string, tf2.value AS my_int1, tf3.value AS my_int2
    subquery << "SELECT ";
    for (size_t i = 0; i < produced_columns.size(); i++) {
        const auto& col = produced_columns[i];
        subquery << "tf" << i << ".value AS " << col.Name();
        if (i < produced_columns.size() - 1) {
            subquery << ", ";
        }
    }
    // FROM random_string() as tf1
    // POSITIONAL JOIN random_int() as tf2
    // POSITIONAL JOIN random_int() as tf3
    subquery << " FROM ";
    for (size_t i = 0; i < produced_columns.size(); i++) {
        const auto& col = produced_columns[i];
        std::string generator = logical_type_to_generator_name(col.Type());
        subquery << generator << "() as tf" << i;
        if (i < produced_columns.size() - 1) {
            subquery << " POSITIONAL JOIN ";
        }
    }

    Parser parser(context.GetParserOptions());
    parser.ParseQuery(subquery.str());
    auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

    return duckdb::make_uniq_base<TableRef, SubqueryRef>(std::move(select_stmt));
}

void RandomDataExecute(ClientContext&, TableFunctionInput&, DataChunk&) {
    // The table function gets replaced with a subquery that calls other generators instead.
    throw InternalException("RandomDataFunction should never be executed directly");
}
} // anonymous namespace

void RandomDataFunction::RegisterFunction(ExtensionLoader& loader) {
    TableFunction random_data_function("random_data", {}, RandomDataExecute);
    random_data_function.bind_replace = RandomDataBindReplace;
    random_data_function.named_parameters["schema_source"] = LogicalType::VARCHAR;
    // TODO: Add support for rowid column and projection pushdown
    loader.RegisterFunction(random_data_function);
}

} // namespace duckdb_faker
