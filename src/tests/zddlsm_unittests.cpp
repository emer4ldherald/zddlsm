#include "gtest/gtest.h"
#include "../zddlsm/zddlsm.h"

TEST(test1, test1) {
    LSMZDD::Storehouse<std::vector<uint32_t>> zdd(64, 3);
    zdd.LSMinsert({0, 1}, 2);
    zdd.LSMinsert({1, 1}, 4);
    zdd.LSMinsert({1, 2342342}, 7);

    EXPECT_EQ(zdd.getLevel({0, 1}).value(), 2);
    EXPECT_EQ(zdd.getLevel({1, 1}).value(), 4);
    EXPECT_EQ(zdd.getLevel({1, 2342342}).value(), 7);

    zdd.LSMdelete({0, 1}, 2);

    EXPECT_EQ(zdd.getLevel({1, 1}).value(), 4);
    EXPECT_EQ(zdd.getLevel({1, 2342342}).value(), 7);
    // EXPECT_FALSE(zdd.getLevel({0, 1}).has_value());

    //EXPECT_TRUE(zdd.Contains({1, 1}).has_value());
    //EXPECT_TRUE(zdd.Contains({1, 2342342}).has_value());
    //EXPECT_FALSE(zdd.Contains({1, 2342341}).has_value());
    // EXPECT_FALSE(zdd.Contains({0xFFFFFFFF, 0}).has_value());
    // std::cout << zdd.getLevel({0, 1}).has_value() << "\n";
    // EXPECT_EQ(zdd.getLevel({1,1}).value(), 1);
    // EXPECT_NE(zdd.getLevel({0,1}).value(), 0);
    // EXPECT_NE(zdd.getLevel({0,1}).value(), 2);
}
