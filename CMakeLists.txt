cmake_minimum_required(VERSION 3.16)
project(LsmTree)


set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g")

add_compile_options(-msse4.2 -mcrc32)

set(SOURCES lsm_tree.cpp
)

add_library(lsm_tree STATIC ${SOURCES})

enable_testing()
add_subdirectory(deps)
add_subdirectory(compact)
add_subdirectory(mvcc)

add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark lsm_tree compact mvcc)