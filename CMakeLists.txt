cmake_minimum_required(VERSION 3.30)
project(unary_prototype_csv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Release)

# Add this line to specify include directories
include_directories(include)

add_subdirectory(ips2ra)
add_subdirectory(ips4o)


file(GLOB AGGREGATOR_CPPS "src/aggregators/**.cpp"
        "src/operators/*.cpp"
        "src/operators/utils/*.cpp")

set(COMMON_SOURCES src/data_io.cpp
        ${AGGREGATOR_CPPS})

#add_executable(unary_prototype_csv src/data_processing.cpp
#        ${COMMON_SOURCES})
add_executable(binary_creator src/binary_creator.cpp
        ${COMMON_SOURCES})
add_executable(binary_prototype_csv src/binary_data_processing.cpp
        ${COMMON_SOURCES})
add_executable(sorting_tester src/sorting_tester.cpp
        ${COMMON_SOURCES})

# Link the ips2ra library to your executables
target_link_libraries(binary_prototype_csv PRIVATE ips2ra)
target_link_libraries(sorting_tester PRIVATE ips2ra)

# Link the ips4o library to your executables
target_link_libraries(binary_prototype_csv PRIVATE ips4o)
target_link_libraries(sorting_tester PRIVATE ips4o)

target_compile_options(sorting_tester PRIVATE -pthread)
target_link_libraries(sorting_tester PRIVATE pthread)

target_compile_options(binary_prototype_csv PRIVATE -pthread)
target_link_libraries(binary_prototype_csv PRIVATE pthread)
