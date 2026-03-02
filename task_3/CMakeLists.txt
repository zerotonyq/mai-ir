cmake_minimum_required(VERSION 3.10)
project(task_3 C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(tokenize src/main.c)

if (NOT MSVC)
    target_link_libraries(tokenize PRIVATE m)
endif()
