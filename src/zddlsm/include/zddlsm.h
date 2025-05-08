#pragma once
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <deque>
#include <optional>
#include <set>
#include <vector>

#include "../../SAPPOROBDD/include/ZBDD.h"

namespace ZDDLSM {
class KeyLevelPair {
private:
    std::string key_;
    uint8_t level_;

public:
    KeyLevelPair(std::string key, uint8_t level) : key_(key), level_(level) {}
    KeyLevelPair() : key_(""), level_(0) {}

    std::string Key() const { return key_; }
    uint8_t Level() const { return level_; }

    bool operator==(const KeyLevelPair& other) const {
        return key_ == other.key_ && level_ == other.level_;
    }
};

/*
LockGuard for concurrent access.

Accessing threads block zdd sequentially.
*/
class ZDDLockGuard {
private:
    static std::atomic<uint32_t> currId_;
    static std::atomic<uint32_t> ready_;

public:
    uint32_t id;

    ZDDLockGuard();

    ~ZDDLockGuard();
};

class ZDDLSMIterator;

class Storehouse {
    ZBDD store;
    uint32_t keyBitLen;
    uint8_t lsmBits_;
    uint32_t bitsForVal;

    bool ProcessZddNode(ZBDD& zdd, std::vector<bddvar>& nz_zdd_vars,
                        int& stack_pointer, int top_var_n);

    static inline ZBDD Child(const ZBDD& n, const int childNum);

    static inline ZBDD LSMKeyTransform(const std::string& key, uint32_t keyLen,
                                       uint8_t lsmBits, uint8_t LSMlev,
                                       uint32_t bitsForVal_);

    std::optional<ZBDD> GetSubZDDbyKey(const std::string& key);

    bool CheckOnLevel(ZBDD base, uint8_t level);

public:
    Storehouse(uint32_t keyLen, uint8_t lsmBits);

    ZDDLockGuard Lock();

    ~Storehouse() = default;

    void Print();

    /*
    Inserts `key` on `level`. If `level` = 1, deletes key from its level and
    inserts it on level 1.

    Unsafe: user must ensure that `key` is placed on `level - 1` before calling,
    otherwise UB.
    */
    void Insert(const std::string &key, uint8_t level);

    /*
    Deletes `key` placed on `level`
    */
    void Delete(const std::string &key, uint8_t level);

    /*
    Returns `std::optional` of current `level` of `key` or `std::nullopt` if
    there's not `key` in zdd.
    */
    std::optional<uint8_t> GetLevel(const std::string &key);

    /*
    For `key` changes its level to (level + 1). Inserts on level `1` if there's
    not `key` in zdd.

    Returns `std::optional` of resulting `key`. If `key` is already on the
    topmost level, returns `std::nullopt`.
    */
    std::optional<uint8_t> PushDown(const std::string& key);

    bool isEmpty();

    static bool isEmpty(ZBDD store);

    friend class ZDDLSMIterator;
};

class ZDDLSMIterator {
private:
    struct ZddNode {
        ZBDD anc;
        int level;
        bool left;
        bool right;
    };

    ZBDD curr_zdd_;
    std::deque<ZddNode> nodes_;
    Storehouse* zdd_;
    bool end_;

    bool TraverseNode(ZddNode*& curr_node, ZBDD& current_zdd, int& curr_level,
                      int& stack_pointer, std::vector<bddvar>& nz_zdd_vars);

    void UpdateCurrentNode(ZddNode*& curr_node, ZBDD& current_zdd,
                           int& curr_level);

public:
    ZDDLSMIterator(Storehouse* zdd, const std::string &key);
    ZDDLSMIterator(ZDDLSM::Storehouse* zdd);

    std::optional<KeyLevelPair> operator*() const;

    void Next();
};
}  // namespace ZDDLSM
