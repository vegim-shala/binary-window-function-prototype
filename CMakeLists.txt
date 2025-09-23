cmake_minimum_required(VERSION 3.30)
project(unary_prototype_csv)

set(CMAKE_CXX_STANDARD 20)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)  # Use Release for better performance
endif()

# Common compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

# Add this line to specify include directories
include_directories(include)

add_subdirectory(ips2ra)
add_subdirectory(ips4o)


file(GLOB AGGREGATOR_CPPS "src/aggregators/**.cpp"
        "src/operators/*.cpp"
        "src/operators/utils/*.cpp")

set(COMMON_SOURCES src/data_io.cpp
        ${AGGREGATOR_CPPS}
)

#add_executable(unary_prototype_csv src/data_processing.cpp
#        ${COMMON_SOURCES})
add_executable(binary_creator src/binary_creator.cpp
        ${COMMON_SOURCES})
add_executable(binary_prototype_csv src/binary_data_processing.cpp
        ${COMMON_SOURCES})
add_executable(sorting_tester src/sorting_tester.cpp
        ${COMMON_SOURCES})
add_executable(partitioning_tester src/partitioning_tester.cpp src/operators/utils/partition_utils.cpp include/operators/utils/partition_utils.h src/data_io.cpp include/data_io.h)
add_executable(benchmark_test src/benchmark_bwf.cpp
        ${COMMON_SOURCES})

# Link the ips2ra library to your executables
target_link_libraries(binary_prototype_csv PRIVATE ips2ra)
target_link_libraries(sorting_tester PRIVATE ips2ra)
target_link_libraries(partitioning_tester PRIVATE ips2ra)
target_link_libraries(benchmark_test PRIVATE ips2ra)

# Link the ips4o library to your executables
target_link_libraries(binary_prototype_csv PRIVATE ips4o)
target_link_libraries(sorting_tester PRIVATE ips4o)
target_link_libraries(partitioning_tester PRIVATE ips4o)
target_link_libraries(benchmark_test PRIVATE ips4o)


target_compile_options(sorting_tester PRIVATE -pthread)
target_link_libraries(sorting_tester PRIVATE pthread)
#
target_compile_options(binary_prototype_csv PRIVATE -pthread)
target_link_libraries(binary_prototype_csv PRIVATE pthread)

target_compile_options(partitioning_tester PRIVATE -pthread)
target_link_libraries(partitioning_tester PRIVATE pthread)

target_compile_options(benchmark_test PRIVATE -pthread)
target_link_libraries(benchmark_test PRIVATE pthread)

# For macOS with Homebrew-installed libomp
set(OPENMP_ROOT "/opt/homebrew/opt/libomp")  # Homebrew installation path

find_path(OpenMP_INCLUDE_DIRS omp.h PATHS ${OPENMP_ROOT}/include)
find_library(OpenMP_CXX_LIBRARIES NAMES omp PATHS ${OPENMP_ROOT}/lib)

if(OpenMP_INCLUDE_DIRS AND OpenMP_CXX_LIBRARIES)
    target_include_directories(partitioning_tester PRIVATE ${OpenMP_INCLUDE_DIRS})
    target_link_libraries(partitioning_tester PRIVATE ${OpenMP_CXX_LIBRARIES})
    target_compile_options(partitioning_tester PRIVATE -Xpreprocessor -fopenmp)

    target_include_directories(binary_prototype_csv PRIVATE ${OpenMP_INCLUDE_DIRS})
    target_link_libraries(binary_prototype_csv PRIVATE ${OpenMP_CXX_LIBRARIES})
    target_compile_options(binary_prototype_csv PRIVATE -Xpreprocessor -fopenmp)

    target_include_directories(benchmark_test PRIVATE ${OpenMP_INCLUDE_DIRS})
    target_link_libraries(benchmark_test PRIVATE ${OpenMP_CXX_LIBRARIES})
    target_compile_options(benchmark_test PRIVATE -Xpreprocessor -fopenmp)
endif()

