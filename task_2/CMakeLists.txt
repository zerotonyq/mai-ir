cmake_minimum_required(VERSION 3.16)
project(crawler_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

pkg_check_modules(MONGOC REQUIRED libmongoc-1.0)
pkg_check_modules(BSON REQUIRED libbson-1.0)

add_executable(crawler
    src/main.cpp
)

target_include_directories(crawler PRIVATE
    ${CURL_INCLUDE_DIRS}
    ${MONGOC_INCLUDE_DIRS}
    ${BSON_INCLUDE_DIRS}
)

target_link_libraries(crawler
    PRIVATE
        ${CURL_LIBRARIES}
        OpenSSL::SSL
        OpenSSL::Crypto
        ${MONGOC_LIBRARIES}
        ${BSON_LIBRARIES}
)

# Some pkg-config libs need extra flags
add_definitions(${MONGOC_CFLAGS_OTHER} ${BSON_CFLAGS_OTHER})
