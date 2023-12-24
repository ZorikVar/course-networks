cmake_minimum_required(VERSION 3.10)
project(pybind_vid)

set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# pybind11
add_subdirectory(extern/pybind11)

# Wise Protocol
pybind11_add_module(wise_protocol protocol.cpp)
target_compile_features(wise_protocol PRIVATE cxx_std_23)
target_compile_options (wise_protocol PRIVATE
    -fdiagnostics-color=always
)

# Cpptrace
include(FetchContent)
FetchContent_Declare(
  cpptrace
  GIT_REPOSITORY https://github.com/jeremy-rifkin/cpptrace.git
  GIT_TAG        v0.3.1 # <HASH or TAG>
)
FetchContent_MakeAvailable(cpptrace)
target_link_libraries(wise_protocol PRIVATE cpptrace::cpptrace)

target_link_libraries  (wise_protocol PRIVATE
    -lstdc++_libbacktrace
)
