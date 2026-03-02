cmake_minimum_required(VERSION 3.10)
project(task_5 CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(ir_cli src/search_cli.cpp)
add_executable(ir_web src/web.cpp)
