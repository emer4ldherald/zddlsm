cmake_minimum_required(VERSION 3.28)
project(zddlsm)

include(ExternalProject)

add_compile_options(-Wall -Wno-overflow -Wextra -Wshadow -DB_64 -O3 -Ofast)

set(SAPPORO_ROOT ${PROJECT_SOURCE_DIR}/SAPPOROBDD)
set(SAPPORO_SRC_DIR ${SAPPORO_ROOT}/src)
set(SAPPORO_LIB ${SAPPORO_ROOT}/lib/libBDD64.so)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ZSTD REQUIRED libzstd)
pkg_check_modules(OPENSSL REQUIRED openssl)

ExternalProject_Add(sapporo_build
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/build_scripts
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
    COMMENT "Building SAPPOROBDD (libBDD64.so and libbddc64.so)..."
)

add_library(sapporo SHARED IMPORTED GLOBAL)

set_target_properties(sapporo PROPERTIES
    IMPORTED_LOCATION ${SAPPORO_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${SAPPORO_ROOT}/include
    #INTERFACE_LINK_LIBRARIES sapporo_c
)

add_dependencies(sapporo sapporo_build)

add_library(zddlsmlib SHARED
            ${PROJECT_SOURCE_DIR}/src/zddlsm/zddlsm.cc
            ${PROJECT_SOURCE_DIR}/src/zddlsm/compression.cc)

target_include_directories(zddlsmlib PUBLIC
    ${PROJECT_SOURCE_DIR}/src/zddlsm/include
    ${ZSTD_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIRS}
)

target_link_libraries(zddlsmlib 
    PUBLIC sapporo
    ${ZSTD_LIBRARIES}
    ${OPENSSL_LIBRARIES}
)

add_executable(rusage_test
    src/rusage_test.cc
)

set_target_properties(rusage_test PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
)

target_link_libraries(rusage_test
    zddlsmlib
)

#
# unit tests
#

add_executable(zddlsm_unittests
    src/tests/main.cc
    src/tests/zddlsm_unittests.cc)

set_target_properties(zddlsm_unittests PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO)

target_include_directories(zddlsm_unittests PRIVATE
    "${PROJECT_SOURCE_DIR}/src")

add_dependencies(zddlsm_unittests
    zddlsmlib)

target_link_libraries(zddlsm_unittests
    zddlsmlib)

#
# GoogleTest - find in packages or download from github and compile on-the-fly.
#

find_package(GTest)
if (GTest_FOUND)
  message(STATUS "Using installed googletest package.")

  target_include_directories(zddlsm_unittests PRIVATE
      ${GTEST_INCLUDE_DIRS})

  target_link_libraries(zddlsm_unittests
      ${GTEST_LIBRARIES})

else()
  message(STATUS "Fallback to googletest downloading on-the-fly.")

  # download googletest
  configure_file(CMakeLists.txt.gtest.in googletest-download/CMakeLists.txt)

  execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
                  RESULT_VARIABLE result
                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
  if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
  endif()

  execute_process(COMMAND ${CMAKE_COMMAND} --build .
                  RESULT_VARIABLE result
                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
  if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
  endif()

  # prevent overriding the parent project's compiler/linker on windows
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

  # add googletest to build. This defines the gtest and gtest_main targets.
  add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                   ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                   EXCLUDE_FROM_ALL)

  target_include_directories(zddlsm_unittests PRIVATE
      "${CMAKE_CURRENT_BINARY_DIR}/googletest-src"
      "${CMAKE_CURRENT_BINARY_DIR}/googletest-src/include")

  target_link_libraries(zddlsm_unittests
      gtest_main)

endif()
