include(FetchContent)

FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip)
FetchContent_MakeAvailable(googletest)

add_executable(pa4_test
        IntHistogram_test.cpp
        JoinOptimizer_test.cpp
)

target_link_libraries(pa4_test PRIVATE GTest::gtest_main db)

include(GoogleTest)
gtest_add_tests(TARGET pa4_test TEST_LIST tests)
foreach (test ${tests})
    set_tests_properties(${test} PROPERTIES TIMEOUT 5)
endforeach ()
