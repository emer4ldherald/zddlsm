#include <fstream>
#include <random>
#include <thread>

#include "../zddlsm/include/zddlsm.h"
#include "gtest/gtest.h"

std::string GenerateKey(size_t count) {
    std::string bytes(count, 0);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(32, 126);

    for (size_t i = 0; i < count; ++i) {
        bytes[i] = static_cast<char>(dist(gen));
    }

    return bytes;
}

TEST(Set, single_insert_works_correctly) {
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

    ZDDLSM::Storage zdd(32);

    EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());

    zdd.Set(keys[0], 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 1);

    zdd.Set(keys[0], 2);
    EXPECT_NE(zdd.GetLevel(keys[0]), 1);
    EXPECT_EQ(zdd.GetLevel(keys[0]), 2);

    for (size_t i = 1; i != 11; ++i) {
        zdd.Set(keys[0], i);
    }
    EXPECT_EQ(zdd.GetLevel(keys[0]), 10);

    zdd.Delete(keys[0]);
    EXPECT_FALSE(zdd.GetLevel(keys[0]).has_value());
}

TEST(Set, zero_prefixed_insert_works_correctly) {
    ZDDLSM::Storage zdd(32);

    std::string key_0 = "\0abcd";

    zdd.Set(key_0, 1);
    zdd.Set("zzzzzz", 1);
    zdd.Set("\0\0a", 1);
    EXPECT_EQ(zdd.GetLevel(key_0).value(), 1);
}

TEST(Set, multiple_insert_works_correctly) {
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

    ZDDLSM::Storage zdd(16);
    uint32_t max_level = std::pow(2, 4) - 1;

    for (auto& key : keys) {
        zdd.Set(key, 1);
    }

    for (auto& key : keys) {
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (auto& key : keys) {
        zdd.Delete(key);
    }

    for (size_t i = 0; i != keys.size(); ++i) {
        for (size_t j = 1; j != i + 2; ++j) {
            zdd.Set(keys[i], j % max_level + (j % max_level == 0));
        }
    }

    for (size_t i = 0; i != keys.size(); ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]),
                  (i + 1) % max_level + ((i + 1) % max_level == 0));
    }
}

TEST(Set, insert_only_1e4_test) {
    ZDDLSM::Storage zdd(20);

    for (uint32_t i = 0; i != 1e4; ++i) {
        std::string str = GenerateKey(20);
        zdd.Set(str, 1);
    }
}

TEST(Set, from_1_to_last_1e3_test) {
    ZDDLSM::Storage zdd(16);
    uint32_t max_level = std::pow(2, 4) - 1;

    std::vector<std::string> keys;

    for (uint32_t i = 0; i != 1e3; ++i) {
        std::string key = std::to_string(i);
        for (size_t j = 0; j != i + 2; ++j) {
            zdd.Set(key, j % max_level);
        }
        keys.push_back(std::move(key));
    }

    for (uint32_t i = 0; i != 1e3; ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]), (i + 1) % max_level);
    }
}

TEST(Set, 1e4_test) {
    ZDDLSM::Storage zdd(20);

    for (uint32_t i = 0; i != 1e4; ++i) {
        std::string str = GenerateKey(20);
        zdd.Set(str, 1);
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

    ZDDLSM::Storage zdd(16);

    for (auto& key : keys) {
        zdd.Set(key, 1);
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (uint32_t i = 1; i != 100; ++i) {
        zdd.Delete(std::string(i, 4));
    }

    for (auto& key : keys) {
        EXPECT_EQ(zdd.GetLevel(key), 1);
    }

    for (uint32_t i = 0; i != 10; ++i) {
        zdd.Delete(keys[i]);
    }

    for (uint32_t i = 0; i != 10; ++i) {
        EXPECT_FALSE(zdd.GetLevel(keys[i]).has_value());
    }

    for (uint32_t i = 10; i != keys.size(); ++i) {
        EXPECT_EQ(zdd.GetLevel(keys[i]), 1);
    }

    for (auto& key : keys) {
        zdd.Delete(key);
    }

    EXPECT_TRUE(zdd.IsEmpty());
}

TEST(Set, concurrent_access_is_correct) {
    ZDDLSM::Storage zdd(16);
    std::vector<std::thread> threads;
    uint32_t threads_number = 8;

    for (uint32_t i = 0; i != threads_number; ++i) {
        threads.emplace_back([&zdd]() {
            ZDDLSM::LockGuard guard = zdd.Lock();
            if (guard.id != 0) {
                EXPECT_TRUE(zdd.GetLevel(std::string(guard.id, 4)).has_value());
                zdd.Delete(std::string(guard.id, 4));
            }
            zdd.Set(std::string(guard.id + 1, 4), 1);
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

TEST(Set, works_with_strings) {
    ZDDLSM::Storage zdd(16);
    std::string str1 = "abcdefghijklmn";
    std::string str2 = "abcdefghijkl";
    std::string str3 = "w";
    std::string not_added_string = "abcdefghijklmno";

    zdd.Set(str1, 1);
    zdd.Set(str2, 1);
    zdd.Set(str3, 1);
    EXPECT_TRUE(zdd.GetLevel(str1).has_value());
    EXPECT_EQ(zdd.GetLevel(str1).value(), 1);
    EXPECT_TRUE(zdd.GetLevel(str2).has_value());
    EXPECT_EQ(zdd.GetLevel(str2).value(), 1);
    EXPECT_TRUE(zdd.GetLevel(str3).has_value());
    EXPECT_EQ(zdd.GetLevel(str3).value(), 1);
    EXPECT_FALSE(zdd.GetLevel(not_added_string).has_value());
}

TEST(Iterator, iterator_inits_with_init_value) {
    ZDDLSM::Storage zdd(16);
    std::string str1 = "abcdefghijklmn";
    zdd.Set(str1, 1);
    ZDDLSM::KeyLevelPair kl(str1, 1);

    ZDDLSM::Iterator it(&zdd, str1);
    std::optional<ZDDLSM::KeyLevelPair> kl2 = *it;
    EXPECT_EQ(kl, kl2);
}

TEST(Iterator, iterator_inits_with_min_larger_value) {
    ZDDLSM::Storage zdd(16);
    std::string str0 = "x";
    std::string str1 = "bbrdefghijklmna";
    std::string str2 = "bbd";
    std::string str3 = "dcdscsdc";
    zdd.Set(str0, 1);
    zdd.Set(str1, 1);
    zdd.Set(str2, 1);
    zdd.Set(str3, 1);

    ZDDLSM::KeyLevelPair kl(str1, 1);

    std::string str = "bbr";

    ZDDLSM::Iterator it(&zdd, str);
    EXPECT_EQ(kl, (*it).value());
}

TEST(Iterator, iterator_begin_ctor_works_properly) {
    ZDDLSM::Storage zdd(16);
    std::string str0 = "x";
    std::string str1 = "bbcdefghijklmna";
    std::string str2 = "bbd";
    std::string str3 = "dcdscsdc";
    zdd.Set(str0, 1);
    zdd.Set(str1, 1);
    zdd.Set(str2, 1);
    zdd.Set(str3, 1);

    ZDDLSM::KeyLevelPair kl0(str0, 1);
    ZDDLSM::KeyLevelPair kl1(str1, 1);
    ZDDLSM::KeyLevelPair kl2(str2, 1);
    ZDDLSM::KeyLevelPair kl3(str3, 1);

    ZDDLSM::Iterator it(&zdd);
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

    ZDDLSM::Storage zdd(32);

    for (std::string& key : keys) {
        zdd.Set(key, 1);
    }

    ZDDLSM::Iterator it(&zdd);

    for (std::string& key : keys) {
        EXPECT_EQ(key, (*it).value().Key());
        it.Next();
    }
}

TEST(Iterator, Next_works_properply_small_test_1) {
    ZDDLSM::Storage zdd(32);

    zdd.Set("a", 1);
    zdd.Set("aa", 1);
    zdd.Set("aaa", 1);
    zdd.Set("aaaa", 1);
    zdd.Set("aaaaa", 1);
    zdd.Set("aaaaaa", 1);

    {
        ZDDLSM::Iterator it(&zdd, "aaa");

        EXPECT_EQ("aaa", (*it).value().Key());
    }

    zdd.Set("aaa", 2);

    {
        ZDDLSM::Iterator it(&zdd, "aaa");

        EXPECT_EQ("aaa", (*it).value().Key());
        EXPECT_EQ(2, (*it).value().Level());
    }
}

TEST(Iterator, Next_works_properply_small_test_2) {
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

    ZDDLSM::Storage zdd(32);

    for (std::string& key : keys) {
        zdd.Set(key, 1);
    }

    for (int i = 0; i != n; ++i) {
        if (i % 2 == 0) {
            zdd.Delete(keys[i]);
        }
    }

    ZDDLSM::Iterator it(&zdd);

    for (int i = 0; i != n; ++i) {
        if (i % 2 != 0) {
            EXPECT_EQ(keys[i], (*it).value().Key());
            it.Next();
        }
    }
}

TEST(ColumnFamilyLogic, zero_column_family) {
    ZDDLSM::Storage zdd(32);

    zdd.Set(0, "key", 1);

    EXPECT_EQ(zdd.GetLevel(0, "key").value(), 1);
}

TEST(ColumnFamilyLogic, column_family_insert_simple_test_1) {
    ZDDLSM::Storage zdd(32);

    zdd.Set(1, "key", 1);

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

    ZDDLSM::Storage zdd(32);

    auto cf = [](uint32_t i) { return std::pow(10, i % 4) + i % 4; };

    for (uint32_t i = 0; i != keys.size(); ++i) {
        zdd.Set(cf(i), keys[i], 1);
    }

    for (uint32_t i = 0; i != keys.size(); ++i) {
        for (uint32_t j = 0; j != 4; ++j) {
            if (j == i % 4) {
                EXPECT_EQ(zdd.GetLevel(cf(j), keys[i]).value(), 1);
            } else {
                EXPECT_FALSE(zdd.GetLevel(cf(j), keys[i]).has_value());
            }
        }
    }
}

TEST(ColumnFamilyLogic, iterator_zero_cf) {
    ZDDLSM::Storage zdd(32);

    zdd.Set(0, "abc", 1);
    zdd.Set(1, "abc", 1);
    zdd.Set(1, "abacaba", 1);

    ZDDLSM::Iterator iter(&zdd, 0);
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

    ZDDLSM::Storage zdd(32);

    auto cf = [](uint32_t i) { return std::pow(2, i % 4); };

    for (uint32_t i = 0; i != keys.size(); ++i) {
        zdd.Set(cf(i), keys[i], 1);
    }

    ZDDLSM::Iterator iter_1(&zdd, cf(0));

    for (uint32_t i = 0; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_1).value().Key(), keys[i]);
        iter_1.Next();
    }

    ZDDLSM::Iterator iter_2(&zdd, cf(1));

    for (uint32_t i = 1; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_2).value().Key(), keys[i]);
        iter_2.Next();
    }

    ZDDLSM::Iterator iter_3(&zdd, cf(2));

    for (uint32_t i = 2; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_3).value().Key(), keys[i]);
        iter_3.Next();
    }

    ZDDLSM::Iterator iter_4(&zdd, cf(3));

    for (uint32_t i = 3; i < keys.size(); i += 4) {
        EXPECT_EQ((*iter_4).value().Key(), keys[i]);
        iter_4.Next();
    }
}

TEST(Compression, zstd) {
    uint32_t key_size = 256;

    Compression::ZstdCompressor compressor;

    uint32_t keys_number = 100;
    std::vector<std::string> keys;

    for (uint32_t i = 0; i != keys_number; ++i) {
        keys.push_back(GenerateKey(key_size));
    }

    double min_ratio = 2;
    double max_ratio = 0;
    double total_size = 0;

    for (auto key : keys) {
        double compressed_size = compressor.Compress(key).size();
        double ratio = static_cast<double>(compressed_size) / key.size();
        min_ratio = std::min(min_ratio, ratio);
        max_ratio = std::max(max_ratio, ratio);
        total_size += compressed_size;
    }

    std::cerr << "zstd compression ratio\n";
    std::cerr << "min ratio: " << min_ratio << "\n";
    std::cerr << "max ratio: " << max_ratio << "\n";
    std::cerr << "avg ratio: " << (total_size / keys_number) / key_size << "\n";
}

TEST(Compression, storage_works_with_zstd) {
    uint32_t key_size = 256;

    ZDDLSM::Storage zdd(key_size, Compression::compression::zstd);

    uint32_t keys_number = 100;
    std::vector<std::string> keys;

    int cf_id = 1;
    int level = 1;

    for (uint32_t i = 0; i != keys_number; ++i) {
        keys.push_back(GenerateKey(key_size));
    }

    for (const std::string& key : keys) {
        zdd.Set(cf_id, key, level);
        EXPECT_EQ(zdd.GetLevel(cf_id, key).value(), level);
    }

    for (const std::string& key : keys) {
        zdd.Set(cf_id, key, level + 1);
        EXPECT_EQ(zdd.GetLevel(cf_id, key).value(), level + 1);
    }
}

TEST(Compression, storage_works_with_md5) {
    uint32_t key_size = 256;

    ZDDLSM::Storage zdd(key_size, Compression::compression::md5);

    uint32_t keys_number = 100;
    std::vector<std::string> keys;

    int cf_id = 1;
    int level = 1;

    for (uint32_t i = 0; i != keys_number; ++i) {
        keys.push_back(GenerateKey(key_size));
    }

    for (const std::string& key : keys) {
        zdd.Set(cf_id, key, level);
        EXPECT_EQ(zdd.GetLevel(cf_id, key).value(), level);
    }

    for (const std::string& key : keys) {
        zdd.Set(cf_id, key, level + 1);
        EXPECT_EQ(zdd.GetLevel(cf_id, key).value(), level + 1);
    }
}

TEST(Compression, storage_works_with_sha256) {
    uint32_t key_size = 256;

    ZDDLSM::Storage zdd(key_size, Compression::compression::sha256);

    uint32_t keys_number = 100;
    std::vector<std::string> keys;

    int cf_id = 1;
    int level = 1;

    for (uint32_t i = 0; i != keys_number; ++i) {
        keys.push_back(GenerateKey(key_size));
    }

    for (const std::string& key : keys) {
        zdd.Set(cf_id, key, level);
        EXPECT_EQ(zdd.GetLevel(cf_id, key).value(), level);
    }

    for (const std::string& key : keys) {
        zdd.Set(cf_id, key, level + 1);
        EXPECT_EQ(zdd.GetLevel(cf_id, key).value(), level + 1);
    }
}

TEST(Storage, multi_storage) {
    uint32_t key_size = 10;
    std::vector<std::unique_ptr<ZDDLSM::Storage>> storages;
    int storages_count = 1000;

    for (int i = 0; i != storages_count; ++i) {
        storages.push_back(std::make_unique<ZDDLSM::Storage>(key_size));
        storages.back()->Set("key", i);
    }

    for (int i = 0; i != storages_count; ++i) {
        EXPECT_EQ(storages[i]->GetLevel("key").value(), i);
    }
    BDD_GC();
}

TEST(ShardedStorage, sharded_storage_works_0) {
    uint32_t key_size = 10;
    uint32_t shards_number = 10;
    ZDDLSM::ShardedStorage ss(key_size, shards_number);

    int keys_number = 10000;
    std::vector<std::string> keys;

    for (int i = 0; i != keys_number; ++i) {
        keys.push_back(GenerateKey(key_size));
    }

    for (int i = 0; i != keys_number; ++i) {
        ss.Set(keys[i], i);
    }

    for (int i = 0; i != keys_number; ++i) {
        EXPECT_EQ(ss.GetLevel(keys[i]).value(), i);
    }
}

TEST(ShardedStorage, sharded_storage_works_1) {
    uint32_t key_size = 10;
    uint32_t shards_number = 1;
    ZDDLSM::ShardedStorage ss(key_size, shards_number);

    int keys_number = 10000;
    std::vector<std::string> keys;

    for (int i = 0; i != keys_number; ++i) {
        keys.push_back(GenerateKey(key_size));
    }

    for (int i = 0; i != keys_number; ++i) {
        ss.Set(keys[i], i);
    }

    for (int i = 0; i != keys_number; ++i) {
        if (i % 2 == 0) {
            ss.Delete(keys[i]);
        }
    }

    for (int i = 0; i != keys_number; ++i) {
        if (i % 2 == 0) {
            EXPECT_FALSE(ss.GetLevel(keys[i]).has_value());
        } else {
            EXPECT_EQ(ss.GetLevel(keys[i]).value(), i);
        }
    }
}
