#include <fstream>
#include <random>
#include <thread>

#include "../zddlsm/include/zddlsm.h"
#include "gtest/gtest.h"

std::string GenerateKey(size_t count) {
    std::string bytes(count, 0);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(5, 126);
    
    for (size_t i = 0; i < count; ++i) {
        bytes[i] = static_cast<char>(dist(gen));
    }
    
    return bytes;
}

TEST(Update, empty_key_crashes_zdd) {
    ZDDLSM::Storehouse zdd(256, 4);
    zdd.GetLevel(0, "");
}

TEST(Update, single_insert_works_correctly) {
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

    zdd.Update(keys[0], 0, 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 1);

    zdd.Update(keys[0], 1, 2);
    EXPECT_NE(zdd.GetLevel(keys[0]), 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 2);

    for (size_t i = 1; i != 11; ++i) {
        zdd.Update(keys[0], i - 1, i);
    }
    EXPECT_EQ(zdd.GetLevel(keys[0]), 10);

    zdd.Delete(keys[0], 10);
    EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());
}

TEST(Update, zero_prefixed_insert_works_correctly) {
    ZDDLSM::Storehouse zdd(256, 4);

    std::string key_0 = "\0abcd";

    zdd.Update(key_0, 0, 1);
    zdd.Update("zzzzzz", 0, 1);
    zdd.Update("\0\0a", 0, 1);
    EXPECT_EQ(zdd.GetLevel(key_0).value(), 1);
}

TEST(Update, multiple_insert_works_correctly) {
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
    uint32_t max_level = std::pow(2, 4) - 1;

    for (auto& key : keys) {
        zdd.Update(key, 0, 1);
    }

    for (auto& key : keys) {
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (auto& key : keys) {
        zdd.Delete(key, 1);
    }

    for (size_t i = 0; i != keys.size(); ++i) {
        for (size_t j = 1; j != i + 2; ++j) {
            zdd.Update(keys[i], zdd.GetLevel(keys[i]).value_or(0), j % max_level + (j % max_level == 0));
        }
    }

    for (size_t i = 0; i != keys.size(); ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]),
                  (i + 1) % max_level + ((i + 1) % max_level == 0));
    }
}

TEST(Update, insert_only_1e4_test) {
    ZDDLSM::Storehouse zdd(160, 4);

    for (uint32_t i = 0; i != 1e4; ++i) {
        std::string str = GenerateKey(20);
        zdd.Update(str, 0, 1);
    }
}

TEST(Update, from_1_to_last_1e3_test) {
    ZDDLSM::Storehouse zdd(128, 4);
    uint32_t max_level = std::pow(2, 4) - 1;

    std::vector<std::string> keys;

    for (uint32_t i = 0; i != 1e3; ++i) {
        std::string key = std::to_string(i);
        for (size_t j = 0; j != i + 2; ++j) {
            zdd.Update(key, zdd.GetLevel(key).value_or(0), j % max_level);
            
        }
        keys.push_back(std::move(key));
    }

    for (uint32_t i = 0; i != 1e3; ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]), (i + 1) % max_level);
    }
}

TEST(Update, 1e4_test) {
    ZDDLSM::Storehouse zdd(160, 4);

    for (uint32_t i = 0; i != 1e4; ++i) {
        std::string str = GenerateKey(20);
        zdd.Update(str, 0, 1);
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
        zdd.Update(key, 0, 1);
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

TEST(Update, concurrent_access_is_correct) {
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
            zdd.Update(std::string(guard.id + 1, 4), 0, 1);
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

TEST(Update, works_with_strings) {
    ZDDLSM::Storehouse zdd(16 * 8, 4);
    std::string str1 = "abcdefghijklmn";
    std::string str2 = "abcdefghijkl";
    std::string str3 = "w";
    std::string not_added_string = "abcdefghijklmno";

    zdd.Update(str1, 0, 1);
    zdd.Update(str2, 0, 1);
    zdd.Update(str3, 0, 1);
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
    zdd.Update(str1, 0, 1);
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
    zdd.Update(str0, 0, 1);
    zdd.Update(str1, 0, 1);
    zdd.Update(str2, 0, 1);
    zdd.Update(str3, 0, 1);

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
    zdd.Update(str0, 0, 1);
    zdd.Update(str1, 0, 1);
    zdd.Update(str2, 0, 1);
    zdd.Update(str3, 0, 1);

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
        zdd.Update(key, 0, 1);
    }

    ZDDLSM::ZDDLSMIterator it(&zdd);

    for (std::string& key : keys) {
        EXPECT_EQ(key, (*it).value().Key());
        it.Next();
    }
}

TEST(Iterator, Next_works_properply_small_test_1) {
    ZDDLSM::Storehouse zdd(32 * 8, 4);

    zdd.Update("a", 0, 1);
    zdd.Update("aa", 0, 1);
    zdd.Update("aaa", 0, 1);
    zdd.Update("aaaa", 0, 1);
    zdd.Update("aaaaa", 0, 1);
    zdd.Update("aaaaaa", 0, 1);

    {
        ZDDLSM::ZDDLSMIterator it(&zdd, "aaa");

        EXPECT_EQ("aaa", (*it).value().Key());
    }

    zdd.Update("aaa", 1, 2);

    {
        ZDDLSM::ZDDLSMIterator it(&zdd, "aaa");

        EXPECT_EQ("aaa", (*it).value().Key());
        EXPECT_EQ(2, (*it).value().Level());
    }
}

TEST(ColumnFamilyLogic, zero_column_family) {
    ZDDLSM::Storehouse zdd(256, 4);

    zdd.Update(0, "key", 0, 1);

    EXPECT_EQ(zdd.GetLevel(0, "key").value(), 1);
}

TEST(ColumnFamilyLogic, column_family_insert_simple_test_1) {
    ZDDLSM::Storehouse zdd(256, 4);

    zdd.Update(1, "key", 0, 1);

    EXPECT_EQ(zdd.GetLevel(1, "key").value(), 1);
    EXPECT_FALSE(zdd.GetLevel(1, "ke").has_value());
    EXPECT_FALSE(zdd.GetLevel(1, "keyy").has_value());
    EXPECT_FALSE(zdd.GetLevel(0, "key").has_value());
    EXPECT_FALSE(zdd.GetLevel(2, "key").has_value());
}

TEST(ColumnFamilyLogic, column_family_insert_simple_test_2) {
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

    auto cf = [](uint32_t i) { return std::pow(10, i % 4) + i % 4; };

    for(uint32_t i = 0; i != keys.size(); ++i) {
        zdd.Update(cf(i), keys[i], 0, 1);
    }

    for(uint32_t i = 0; i != keys.size(); ++i) {
        for(uint32_t j = 0; j != 4; ++j) {
            if(j == i % 4) {
                EXPECT_EQ(zdd.GetLevel(cf(j), keys[i]).value(), 1);
            } else {
                EXPECT_FALSE(zdd.GetLevel(cf(j), keys[i]).has_value());
            }
        }
    }
}

TEST(ColumnFamilyLogic, iterator_zero_cf) {
    ZDDLSM::Storehouse zdd(32 * 8, 4);

    zdd.Update(0, "abc", 0, 1);
    zdd.Update(1, "abc", 0, 1);
    zdd.Update(1, "abacaba", 0, 1);

    ZDDLSM::ZDDLSMIterator iter(&zdd, 0);
    EXPECT_EQ((*iter).value().Key(), "abc");
}

TEST(ColumnFamilyLogic, iterator_works_with_only_one_column_family) {
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

    auto cf = [](uint32_t i) { return std::pow(2, i % 4); };

    for(uint32_t i = 0; i != keys.size(); ++i) {
        zdd.Update(cf(i), keys[i], 0, 1);
    }
    
    ZDDLSM::ZDDLSMIterator iter_1(&zdd, cf(0));

    for(uint32_t i = 0; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_1).value().Key(), keys[i]);
        iter_1.Next();
    }

    ZDDLSM::ZDDLSMIterator iter_2(&zdd, cf(1));

    for(uint32_t i = 1; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_2).value().Key(), keys[i]);
        iter_2.Next();
    }

    ZDDLSM::ZDDLSMIterator iter_3(&zdd, cf(2));

    for(uint32_t i = 2; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_3).value().Key(), keys[i]);
        iter_3.Next();
    }

    ZDDLSM::ZDDLSMIterator iter_4(&zdd, cf(3));

    for(uint32_t i = 3; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_4).value().Key(), keys[i]);
        iter_4.Next();
    }
}

