#include <fstream>
#include <thread>

#include "../zddlsm/include/zddlsm.h"
#include "gtest/gtest.h"

TEST(Insert, single_insert_works_correctly) {
    std::ifstream file;
    file.open("../src/tests/files/lex_sorted_strings_256.txt");

    EXPECT_TRUE(file.is_open());

    int n = 0;
    file >> n;
    std::vector<std::string> keys;

    for (int i = 0; i != n; ++i) {
        std::string key;
        file >> key;
        keys.push_back(key);
    }

    ZDDLSM::Storehouse zdd(256, 4);

    EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());

    zdd.Insert(keys[0], 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 1);

    zdd.Insert(keys[0], 2);
    EXPECT_NE(zdd.GetLevel(keys[0]), 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 2);

    for (size_t i = 1; i != 11; ++i) {
        zdd.Insert(keys[0], i);
    }
    EXPECT_EQ(zdd.GetLevel(keys[0]), 10);

    zdd.Delete(keys[0], 10);
    EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());
}

TEST(Insert, single_insert_works_correctly_when_adding_to_lev_1) {
    std::ifstream file;
    file.open("../src/tests/files/lex_sorted_strings_256.txt");

    EXPECT_TRUE(file.is_open());

    int n = 0;
    file >> n;
    std::vector<std::string> keys;

    for (int i = 0; i != n; ++i) {
        std::string key;
        file >> key;
        keys.push_back(key);
    }

    ZDDLSM::Storehouse zdd(256, 4);

    for (size_t i = 1; i != 11; ++i) {
        zdd.Insert(keys[0], i);
    }
    EXPECT_EQ(zdd.GetLevel(keys[0]), 10);

    zdd.Insert(keys[0], 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 1);

    zdd.Insert(keys[0], 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 1);
}

TEST(Insert, multiple_insert_works_correctly) {
    std::ifstream file;
    file.open("../src/tests/files/lex_sorted_strings_256.txt");

    EXPECT_TRUE(file.is_open());

    int n = 0;
    file >> n;
    std::vector<std::string> keys;

    for (int i = 0; i != n; ++i) {
        std::string key;
        file >> key;
        keys.push_back(key);
    }

    ZDDLSM::Storehouse zdd(128, 4);
    uint32_t max_level = std::pow(2, 4);

    for (auto& key : keys) {
        zdd.Insert(key, 1);
    }

    for (auto& key : keys) {
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (auto& key : keys) {
        zdd.Delete(key, 1);
    }

    for (size_t i = 0; i != keys.size(); ++i) {
        for (size_t j = 1; j != i + 2; ++j) {
            zdd.Insert(keys[i], j % max_level + (j % max_level == 0));
        }
    }

    for (size_t i = 0; i != keys.size(); ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]),
                  (i + 1) % max_level + ((i + 1) % max_level == 0));
    }
}

TEST(Insert, 1e3_test) {
    ZDDLSM::Storehouse zdd(128, 4);
    uint32_t max_level = std::pow(2, 4);

    for (uint32_t i = 0; i != 1e3; ++i) {
        for (size_t j = 1; j != i + 2; ++j) {
            zdd.Insert(std::string(4, i), j % max_level + (j % max_level == 0));
        }
    }

    for (uint32_t i = 0; i != 1e3; ++i) {
        EXPECT_EQ(zdd.GetLevel(std::string(4, i)),
                  (i + 1) % max_level + ((i + 1) % max_level == 0));
    }
}

TEST(PushDown, push_down_works_correctly) {
    std::ifstream file;
    file.open("../src/tests/files/lex_sorted_strings_256.txt");

    EXPECT_TRUE(file.is_open());

    int n = 0;
    file >> n;
    std::vector<std::string> keys;

    for (int i = 0; i != n; ++i) {
        std::string key;
        file >> key;
        keys.push_back(key);
    }

    ZDDLSM::Storehouse zdd(128, 4);
    uint32_t max_level = std::pow(2, 4);

    for (uint32_t i = 1; i != max_level; ++i) {
        for (auto& key : keys) {
            zdd.PushDown(key);
            EXPECT_EQ(zdd.GetLevel(key), i);
        }
    }
}

TEST(Delete, delete_is_correct) {
    std::ifstream file;
    file.open("../src/tests/files/lex_sorted_strings_256.txt");

    EXPECT_TRUE(file.is_open());

    int n = 0;
    file >> n;
    std::vector<std::string> keys;

    for (int i = 0; i != n; ++i) {
        std::string key;
        file >> key;
        keys.push_back(key);
    }

    ZDDLSM::Storehouse zdd(128, 4);

    for (auto& key : keys) {
        zdd.PushDown(key);
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (uint32_t i = 1; i != 100; ++i) {
        zdd.Delete(std::string(i, 4), 1);
    }

    for (auto& key : keys) {
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (uint32_t i = 0; i != 10; ++i) {
        zdd.Delete(keys[i], 1);
    }

    for (uint32_t i = 0; i != 10; ++i) {
        EXPECT_FALSE(zdd.GetLevel(keys[i]).has_value());
    }

    for (uint32_t i = 10; i != keys.size(); ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]), 1);
    }

    for (auto& key : keys) {
        zdd.Delete(key, 1);
    }

    EXPECT_TRUE(zdd.isEmpty());
}

TEST(Insert, concurrent_access_is_correct) {
    ZDDLSM::Storehouse zdd(128, 4);
    std::vector<std::thread> threads;
    uint32_t threads_number = 8;

    for (uint32_t i = 0; i != threads_number; ++i) {
        threads.emplace_back([&zdd]() {
            ZDDLSM::ZDDLockGuard guard = zdd.Lock();
            if (guard.id != 0) {
                EXPECT_TRUE(zdd.GetLevel(std::string(guard.id, 4)).has_value());
                zdd.Delete(std::string(guard.id, 4), 1);
            }
            zdd.Insert(std::string(guard.id + 1, 4), 1);
        });
    }

    for (std::thread& t : threads) {
        t.join();
    }

    for (uint32_t i = 1; i != threads_number + 1; ++i) {
        if (i != threads_number) {
            EXPECT_FALSE(zdd.GetLevel(std::string(i, 4)).has_value());
        } else {
            EXPECT_TRUE(zdd.GetLevel(std::string(i, 4)).has_value());
        }
    }
}

TEST(Insert, works_with_strings) {
    ZDDLSM::Storehouse zdd(16 * 8, 4);
    std::string str1 = "abcdefghijklmn";
    std::string str2 = "abcdefghijkl";
    std::string str3 = "w";
    std::string not_added_string = "abcdefghijklmno";

    zdd.Insert(str1, 1);
    zdd.Insert(str2, 1);
    zdd.Insert(str3, 1);
    EXPECT_TRUE(zdd.GetLevel(str1).has_value());
    EXPECT_EQ(zdd.GetLevel(str1).value(), 1);
    EXPECT_TRUE(zdd.GetLevel(str2).has_value());
    EXPECT_EQ(zdd.GetLevel(str2).value(), 1);
    EXPECT_TRUE(zdd.GetLevel(str3).has_value());
    EXPECT_EQ(zdd.GetLevel(str3).value(), 1);
    EXPECT_FALSE(zdd.GetLevel(not_added_string).has_value());
}

TEST(Iterator, iterator_inits_with_init_value) {
    ZDDLSM::Storehouse zdd(16 * 8, 4);
    std::string str1 = "abcdefghijklmn";
    zdd.Insert(str1, 1);
    ZDDLSM::KeyLevelPair kl(str1, 1);

    ZDDLSM::ZDDLSMIterator it(&zdd, str1);
    std::optional<ZDDLSM::KeyLevelPair> kl2 = *it;
    EXPECT_EQ(kl, kl2);
}

TEST(Iterator, iterator_inits_with_min_larger_value) {
    ZDDLSM::Storehouse zdd(16 * 8, 4);
    std::string str0 = "x";
    std::string str1 = "bbrdefghijklmna";
    std::string str2 = "bbd";
    std::string str3 = "dcdscsdc";
    zdd.Insert(str0, 1);
    zdd.Insert(str1, 1);
    zdd.Insert(str2, 1);
    zdd.Insert(str3, 1);

    ZDDLSM::KeyLevelPair kl(str1, 1);

    std::string str = "bbr";

    ZDDLSM::ZDDLSMIterator it(&zdd, str);
    EXPECT_EQ(kl, (*it).value());
}

TEST(Iterator, iterator_begin_ctor_works_properly) {
    ZDDLSM::Storehouse zdd(16 * 8, 4);
    std::string str0 = "x";
    std::string str1 = "bbcdefghijklmna";
    std::string str2 = "bbd";
    std::string str3 = "dcdscsdc";
    zdd.Insert(str0, 1);
    zdd.Insert(str1, 1);
    zdd.Insert(str2, 1);
    zdd.Insert(str3, 1);

    ZDDLSM::KeyLevelPair kl0(str0, 1);
    ZDDLSM::KeyLevelPair kl1(str1, 1);
    ZDDLSM::KeyLevelPair kl2(str2, 1);
    ZDDLSM::KeyLevelPair kl3(str3, 1);

    ZDDLSM::ZDDLSMIterator it(&zdd);
    EXPECT_EQ(kl1, (*it).value());
    it.Next();
    EXPECT_EQ(kl2, (*it).value());
    it.Next();
    EXPECT_EQ(kl3, (*it).value());
    it.Next();
    EXPECT_EQ(kl0, (*it).value());
    it.Next();
    EXPECT_EQ(std::nullopt, (*it));
}

TEST(Iterator, Next_works_properply_small_test_0) {
    std::ifstream file;
    file.open("../src/tests/files/lex_sorted_strings_256.txt");

    EXPECT_TRUE(file.is_open());

    int n = 0;
    file >> n;
    std::vector<std::string> keys;

    for (int i = 0; i != n; ++i) {
        std::string key;
        file >> key;
        keys.push_back(key);
    }

    ZDDLSM::Storehouse zdd(32 * 8, 4);

    for (std::string& key : keys) {
        zdd.Insert(key, 1);
    }

    ZDDLSM::ZDDLSMIterator it(&zdd);

    for (std::string& key : keys) {
        EXPECT_EQ(key, (*it).value().Key());
        it.Next();
    }
}

TEST(Iterator, Next_works_properply_small_test_1) {
    ZDDLSM::Storehouse zdd(32 * 8, 4);

    zdd.Insert("a", 1);
    zdd.Insert("aa", 1);
    zdd.Insert("aaa", 1);
    zdd.Insert("aaaa", 1);
    zdd.Insert("aaaaa", 1);
    zdd.Insert("aaaaaa", 1);

    {
        ZDDLSM::ZDDLSMIterator it(&zdd, "aaa");

        EXPECT_EQ("aaa", (*it).value().Key());
    }

    zdd.Insert("aaa", 2);

    {
        ZDDLSM::ZDDLSMIterator it(&zdd, "aaa");

        EXPECT_EQ("aaa", (*it).value().Key());
        EXPECT_EQ(2, (*it).value().Level());
    }
}