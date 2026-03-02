cmake_minimum_required(VERSION 3.10)
project(task_4 CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(MONGOC REQUIRED libmongoc-1.0)

include_directories(${MONGOC_INCLUDE_DIRS})
link_directories(${MONGOC_LIBRARY_DIRS})

add_executable(ir_indexer src/indexer.cpp)
target_link_libraries(ir_indexer ${MONGOC_LIBRARIES})
