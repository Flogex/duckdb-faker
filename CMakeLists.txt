cmake_minimum_required(VERSION 3.22)
set (CMAKE_CXX_STANDARD 20)

# Set extension name here
set(TARGET_NAME faker)
project(DuckDBFakerExtension)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

add_subdirectory(submodules/duckdb)

set(BUILD_TESTING OFF)
add_subdirectory(submodules/faker-cxx)

set(SOURCE_FILES
    src/faker_extension.cpp
    src/table_functions/numbers.cpp)

build_static_extension(${TARGET_NAME} ${SOURCE_FILES})
build_loadable_extension(${TARGET_NAME} " " ${SOURCE_FILES})

set(INCLUDES
    src/include
    ${CMAKE_CURRENT_LIST_DIR}/submodules/faker-cxx/include)

target_include_directories(${EXTENSION_NAME} PRIVATE ${INCLUDES})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${INCLUDES})

target_link_libraries(${EXTENSION_NAME} faker-cxx)
target_link_libraries(${LOADABLE_EXTENSION_NAME} faker-cxx)

install(
    TARGETS faker-cxx
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

add_subdirectory(test)