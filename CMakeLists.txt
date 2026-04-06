cmake_minimum_required(VERSION 3.14)
project(chrona LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(json)

add_executable(chrona
    src/main.cpp
    src/model.cpp
    src/ordering.cpp
    src/window.cpp
    src/causal.cpp
    src/engine.cpp
    src/persistence.cpp
)
target_include_directories(chrona PRIVATE include)
target_link_libraries(chrona PRIVATE nlohmann_json::nlohmann_json)
