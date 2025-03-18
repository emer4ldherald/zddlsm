#include "../zddlsm/zddlsm.h"
#include "gtest/gtest.h"

static const std::vector<std::vector<uint32_t>> keys = {{0xDEADBEEF, 0xFFFFFFFF, 0xCAFEBABE, 0xDEADBABE},
                                                        {1960499302, 2398538437, 461365412, 3685129574},
                                                        {2012209595, 2952931182, 3786484839, 985220384},
                                                        {657548570, 2058122981, 665992475, 1354496225},
                                                        {2134718702, 2665014591, 3156183718, 529045074},
                                                        {2605303930, 1365358545, 3129987772, 4166297087},
                                                        {3194475395, 2895250220, 1961600152, 2317447229},
                                                        {2579017435, 4034045931, 1342921522, 3861235644},
                                                        {1557326054, 807205751, 2583406806, 3114939605},
                                                        {3075150562, 2406369693, 311593713, 2080262591},
                                                        {1114734844, 3434331173, 2781355878, 1266304737},
                                                        {799487201, 4095164603, 1958724538, 1233756383},
                                                        {227913389, 4007455488, 1567035738, 3279011349},
                                                        {2027028414, 56580478, 3975519477, 1139133530},
                                                        {2937033998, 2147101492, 2111336342, 693336356},
                                                        {2684505722, 1235186478, 2090748607, 3729014034},
                                                        {1648928534, 1866065706, 2516279572, 3616662339},
                                                        {2301265788, 2979267494, 2191313675, 1178907322},
                                                        {341493128, 2705053826, 2584736845, 2860168363},
                                                        {2499584465, 2330355703, 2526365318, 677993374}};

TEST(Insert, single_insert_works_correctly) {
  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(128, 4);

  EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());

  zdd.Insert(keys[0], 1);
  EXPECT_EQ(zdd.GetLevel(keys[0]), 1);

  zdd.Insert(keys[0], 2);
  EXPECT_NE(zdd.GetLevel(keys[0]), 1);
  EXPECT_EQ(zdd.GetLevel(keys[0]), 2);

  for(size_t i = 1; i != 11; ++i) {
    zdd.Insert(keys[0], i);
  }
  EXPECT_EQ(zdd.GetLevel(keys[0]), 10);

  zdd.Delete(keys[0], 10);
  EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());
}

TEST(Insert, single_insert_works_correctly_when_adding_to_lev_1) {
  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(128, 4);
  for(size_t i = 1; i != 11; ++i) {
    zdd.Insert(keys[0], i);
  }
  EXPECT_EQ(zdd.GetLevel(keys[0]), 10);

  zdd.Insert(keys[0], 1);
  EXPECT_EQ(zdd.GetLevel(keys[0]), 1);

  zdd.Insert(keys[0], 1);
  EXPECT_EQ(zdd.GetLevel(keys[0]), 1);
}

TEST(Insert, multiple_insert_works_correctly) {
  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(128, 4);
  uint32_t max_level = std::pow(2, 4);

  for(std::vector<uint32_t> key : keys) {
    zdd.Insert(key, 1);
  }

  for(std::vector<uint32_t> key : keys) {
    EXPECT_EQ(zdd.GetLevel(key), 1);
  }

  for(std::vector<uint32_t> key : keys) {
    zdd.Delete(key, 1);
  }

  for(size_t i = 0; i != keys.size(); ++i) {
    for(size_t j = 1; j != i + 2; ++j) {
      zdd.Insert(keys[i], j % max_level + (j % max_level == 0));
    }
  }

  for(size_t i = 0; i != keys.size(); ++i) {
    EXPECT_EQ(zdd.GetLevel(keys[i]), (i + 1) % max_level + ((i + 1) % max_level == 0));
  }
}

TEST(Insert, 1e3_test) {
  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(128, 4);
  uint32_t max_level = std::pow(2, 4);

  for(uint32_t i = 0; i != 1e3; ++i) {
    for(size_t j = 1; j != i + 2; ++j) {
      zdd.Insert({i, i, i, i}, j % max_level + (j % max_level == 0));
    }
  }

  for(uint32_t i = 0; i != 1e3; ++i) {
    EXPECT_EQ(zdd.GetLevel({i, i, i, i}), (i + 1) % max_level + ((i + 1) % max_level == 0));
  }
}

TEST(PushDown, push_down_works_correctly) {
  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(128, 4);
  uint32_t max_level = std::pow(2, 4);

  for(uint32_t i = 1; i != max_level; ++i) {
    for(std::vector<uint32_t> key : keys) {
      zdd.PushDown(key);
      EXPECT_EQ(zdd.GetLevel(key), i);
    }
  }
}

TEST(Delete, delete_is_correct) {
  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(128, 4);
  uint32_t max_level = std::pow(2, 4);

  for(std::vector<uint32_t> key : keys) {
    zdd.PushDown(key);
    EXPECT_EQ(zdd.GetLevel(key), 1);
  }

  for(uint32_t i = 1; i != 100; ++i) {
    zdd.Delete({i, i, i, i}, 1);
  }

  for(std::vector<uint32_t> key : keys) {
    EXPECT_EQ(zdd.GetLevel(key), 1);
  }

  for(uint32_t i = 0; i != 10; ++i) {
    zdd.Delete(keys[i], 1);
  }

  for(uint32_t i = 0; i != 10; ++i) {
    EXPECT_FALSE(zdd.GetLevel(keys[i]).has_value());
  }

  for(uint32_t i = 10; i != keys.size(); ++i) {
    EXPECT_EQ(zdd.GetLevel(keys[i]), 1);
  }

  for(std::vector<uint32_t> key : keys) {
    zdd.Delete(key, 1);
  }

  EXPECT_TRUE(zdd.isEmpty());
}
