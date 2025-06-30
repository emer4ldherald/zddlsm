cmake_minimum_required(VERSION 3.28)

project(zddlsm)

add_compile_options(-Wall -Wextra -Wshadow -DB_64 -O3 -Ofast)

add_library(sapporo STATIC IMPORTED)
set_target_properties(sapporo PROPERTIES
    IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/src/SAPPOROBDD/lib/BDD64.a
    INTERFACE_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR}/src/SAPPOROBDD/include
)

add_library(zddlsmlib 
            ${PROJECT_SOURCE_DIR}/src/zddlsm/zddlsm.cpp)

target_include_directories(zddlsmlib PUBLIC
    ${PROJECT_SOURCE_DIR}/src/zddlsm/include
)

target_link_libraries(zddlsmlib PUBLIC sapporo)

add_executable(zddlsm
    src/main.cpp
)

set_target_properties(zddlsm PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
)

target_link_libraries(zddlsm
    zddlsmlib
)

#
# unit tests
#

add_executable(zddlsm_unittests
    src/tests/main.cpp
    src/tests/zddlsm_unittests.cpp)

set_target_properties(zddlsm_unittests PROPERTIES
    CXX_STANDARD 17
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
