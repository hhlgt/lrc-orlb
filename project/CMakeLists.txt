cmake_minimum_required(VERSION 3.15)
project(ecproject)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--rpath=/home/GuanTian/local/glibc-2.18/lib -Wl,--dynamic-linker=/home/GuanTian/local/glibc-2.18/lib/ld-2.18.so")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--rpath=/home/GuanTian/local/glibc-2.18/lib -Wl,--dynamic-linker=/home/GuanTian/local/glibc-2.18/lib/ld-2.18.so")

find_package(Threads REQUIRED)

# set the search path of include files
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/erasure_codes
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/yalantinglibs/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/gf-complete/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/jerasure/include/jerasure
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/jerasure/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/hiredis/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/redis-plus-plus/include
)

# set the search path of lib files
link_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/jerasure/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/gf-complete/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/hiredis/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/redis-plus-plus/lib64
)

# set the shared source files
set(SHARED_SOURCES
    src/utils.cpp
    src/tinyxml2.cpp
    src/erasure_codes/common.cpp
    src/erasure_codes/lrc.cpp
)

add_executable(run_client
    src/client/client.cpp
    src/client/run_client.cpp
    ${SHARED_SOURCES}
)

add_executable(run_coordinator
    src/coordinator/auxs.cpp
    src/coordinator/placement.cpp
    src/coordinator/repair.cpp
    src/coordinator/migration.cpp
    src/coordinator/coordinator.cpp
    src/coordinator/run_coordinator.cpp
    ${SHARED_SOURCES}
)

add_executable(run_proxy
    src/proxy/proxy.cpp
    src/proxy/handle_repair.cpp
    src/proxy/run_proxy.cpp
    ${SHARED_SOURCES}
)

add_executable(run_datanode
    src/datanode/datanode.cpp
    src/datanode/run_datanode.cpp
    ${SHARED_SOURCES}
)

add_executable(simulation
    src/client/client.cpp
    src/client/simulation.cpp
    ${SHARED_SOURCES}
)

target_link_libraries(run_client Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(run_coordinator Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(run_proxy Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(run_datanode Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(simulation Threads::Threads Jerasure gf_complete redis++)