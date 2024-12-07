cmake_minimum_required(VERSION 3.22)

set(BUILD_TESTING OFF)
set(CATCH_INSTALL_DOCS OFF)
set(CATCH_DIR ${CMAKE_CURRENT_LIST_DIR}/../submodules/catch2)
add_subdirectory(${CATCH_DIR} ${CMAKE_CURRENT_BINARY_DIR}/catch2)

set(DUCKDB_DIR ${CMAKE_CURRENT_LIST_DIR}/../submodules/duckdb)

add_executable(unittests test_numbers.cpp)
target_include_directories(unittests PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../src/include
    ${DUCKDB_DIR}/src/include
    ${CATCH_DIR}/src)
target_link_libraries(unittests PRIVATE
    faker_extension
    duckdb_static
    Catch2::Catch2WithMain)
