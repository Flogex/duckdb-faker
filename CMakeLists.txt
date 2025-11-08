cmake_minimum_required(VERSION 3.22)
set(CMAKE_CXX_STANDARD 20)

# Set extension name here
set(TARGET_NAME faker)
project(DuckDBFakerExtension)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(SOURCE_FILES
    src/faker_extension.cpp
    src/table_functions/booleans.cpp
    src/table_functions/generator_global_state.cpp
    src/table_functions/numbers.cpp
    src/table_functions/rowid_generator.cpp
    src/table_functions/strings.cpp)

set(INCLUDES
    src/include
    src)

add_subdirectory(submodules/duckdb ${CMAKE_BINARY_DIR}/duckdb)
# We need to get DUCKDB_MODULE_BASE_DIR and DUCKDB_NORMALIZED_VERSION into this scope
# for the call of the append_metadata.cmake script to succeed
set(DUCKDB_MODULE_BASE_DIR ${DuckDB_SOURCE_DIR})
get_directory_property(DUCKDB_NORMALIZED_VERSION DIRECTORY "${DuckDB_SOURCE_DIR}" DEFINITION DUCKDB_NORMALIZED_VERSION)

build_static_extension(${TARGET_NAME} ${SOURCE_FILES})
set(PARAMETERS " ")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${SOURCE_FILES})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")

target_include_directories(${EXTENSION_NAME} PRIVATE ${INCLUDES} PRIVATE SYSTEM submodules/faker-cxx/include)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${INCLUDES} PRIVATE SYSTEM submodules/faker-cxx/include)

set(BUILD_TESTING OFF)
add_subdirectory(submodules/faker-cxx)

# The DuckDB build_extension functions used the plain signature already
target_link_libraries(${EXTENSION_NAME} faker-cxx)
target_link_libraries(${LOADABLE_EXTENSION_NAME} faker-cxx)

add_subdirectory(test)

add_dependencies(${LOADABLE_EXTENSION_NAME} duckdb shell)
add_custom_command(TARGET ${LOADABLE_EXTENSION_NAME} POST_BUILD
    COMMAND ${PROJECT_BINARY_DIR}/duckdb/duckdb -c "FORCE INSTALL '${PROJECT_BINARY_DIR}/${TARGET_NAME}.duckdb_extension'"
    COMMENT "Installing Faker extension in DuckDB"
    VERBATIM
)