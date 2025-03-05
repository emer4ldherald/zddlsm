#include "../zddlsm/zddlsm.h"
#include "gtest/gtest.h"

TEST(Insert, level_changes_correctly) {
  LSMZDD::Storehouse<std::vector<uint32_t>> zdd(64, 4);
  for (size_t i = 1; i < std::pow(2, 4); ++i) {
    zdd.Insert({12342342, 213213442}, i);
    EXPECT_EQ(zdd.GetLevel({12342342, 213213442}).value(), i);
    EXPECT_EQ(zdd.GetLevel({12342342, 213213442}).value(), i);
  }

  for (size_t i = 1; i < std::pow(2, 4); ++i) {
    zdd.PushDown({93842348, 212355511});
    EXPECT_EQ(zdd.GetLevel({93842348, 212355511}).value(), i);
  }

  for (uint32_t i = 1; i < std::pow(2, 16); ++i) {
    zdd.PushDown({i, i});
    EXPECT_EQ(zdd.GetLevel({i, i}).value(), 1);
  }
}