#include "../zddlsm/include/zddlsm.h"
#include "gtest/gtest.h"

class ZDDLSMSetup : public ::testing::Environment {
public:
    void SetUp() override {
        int var_nubmer = 10000;
        // calls BDDSystem::InitOnce
        ZDDLSM::Storage zdd(var_nubmer / 8);
    }

    void TearDown() override {}
};

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new ZDDLSMSetup);
    return RUN_ALL_TESTS();
}