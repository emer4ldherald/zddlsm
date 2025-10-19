#pragma once
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <deque>
#include <optional>
#include <set>
#include <vector>

#include "../../../SAPPOROBDD/include/ZBDD.h"

namespace ZDDLSM {
class KeyLevelPair {
public:
    KeyLevelPair(std::string key, uint8_t level)
        : key_(std::move(key)), level_(level) {}
    KeyLevelPair() : key_(""), level_(0) {}

    std::string Key() const { return key_; }
    uint8_t Level() const { return level_; }

    bool operator==(const KeyLevelPair& other) const {
        return key_ == other.key_ && level_ == other.level_;
    }

private:
    std::string key_;
    uint8_t level_;
};

/*
LockGuard for concurrent access.

Accessing threads block zdd sequentially.
*/
class ZDDLockGuard {
public:
    uint32_t id;

    ZDDLockGuard() = delete;

    ZDDLockGuard(std::atomic<uint32_t>& curr_task_id,
                 std::atomic<uint32_t>& ready_task);

    ~ZDDLockGuard();

private:
    std::atomic<uint32_t>& curr_task_id_;
    std::atomic<uint32_t>& ready_task_id_;
};

/*
Internal representation of a key. Contains column family information.

Be sure that `key` lifetime is longer that its ZDD internal representation.
*/
class ZddInternalKey {
public:
    ZddInternalKey(const std::string& key);

    ZddInternalKey(const std::string& key, uint32_t cf_id);

    const std::string &UserKey() const { return key_; }
    
    uint32_t CfID() const { return cf_id_; }

    char operator[](uint32_t index) const;

private:
    const std::string& key_;
    uint32_t cf_id_;
    uint32_t total_size_;
    bool has_cf_;
};

class ZDDLSMIterator;

class Storehouse {
public:
    Storehouse(uint32_t key_len, uint8_t lsm_bits);

    ZDDLockGuard Lock();

    ~Storehouse() = default;

    void Print();

    /*
    Inserts `key` to `to_level`.
    */
    void Insert(const std::string& key, uint8_t to_level);

    void Insert(uint32_t cf_id, const std::string& key, uint8_t to_level);

    /*
    Inserts `key` to `to_level`, deletes `key` in `from_level`.
    */
    void Update(const std::string& key, uint8_t from_level, uint8_t to_level);

    void Update(uint32_t cf_id, const std::string& key, uint8_t from_level,
                uint8_t to_level);

    /*
    Deletes `key` placed on `level`
    */
    void Delete(const std::string& key, uint8_t level);

    void Delete(uint32_t cf_id, const std::string& key, uint8_t level);

    /*
    Returns `std::optional` of current `level` of `key` or `std::nullopt` if
    there's not `key` in zdd.
    */
    std::optional<uint8_t> GetLevel(const std::string& key);

    std::optional<uint8_t> GetLevel(uint32_t cf_id, const std::string& key);

    bool isEmpty();

    static bool isEmpty(ZBDD store);

private:
    ZBDD store_;
    uint32_t key_bit_len_;
    uint8_t lsm_bits_;
    uint32_t bits_for_val_;
    uint32_t max_level_;

    std::atomic<uint32_t> curr_task_id_;
    std::atomic<uint32_t> ready_task_id_;

    std::vector<bddvar> nz_zdd_vars_;

    bool ProcessZddNode(ZBDD& zdd,
                        int& stack_pointer, int top_var_n);

    void GetNzZddVars(const ZddInternalKey& zdd_ikey,
                      uint32_t prefix_len = 0xFFFFFFFF);

    bool AllowsLevel(uint32_t level);

    static inline ZBDD Child(const ZBDD& n, const int child_num);

    inline ZBDD LSMKeyTransform(const ZddInternalKey& key, uint8_t lsm_lev);

    std::optional<ZBDD> GetSubZDDbyKey(const ZddInternalKey& key,
                                       uint32_t prefix_len = 0xFFFFFFFF);
    
    void InsertImpl(const ZddInternalKey& ikey, uint8_t to_level);

    void UpdateImpl(const ZddInternalKey& ikey, uint8_t from_level,
                    uint8_t to_level);

    void DeleteImpl(const ZddInternalKey& ikey, uint8_t level);

    std::optional<uint8_t> GetLevelImpl(const ZddInternalKey& ikey);

    friend class ZDDLSMIterator;
};

class ZDDLSMIterator {
public:
    ZDDLSMIterator(Storehouse* zdd, const std::string& key);
    ZDDLSMIterator(Storehouse* zdd, uint32_t cf_id, const std::string& key);
    ZDDLSMIterator(ZDDLSM::Storehouse* zdd);
    ZDDLSMIterator(ZDDLSM::Storehouse* zdd, uint32_t cf_id);

    std::optional<KeyLevelPair> operator*() const;

    void Next();

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

    void Init(const std::string& key, ZBDD& initial_zdd);

    bool TraverseNode(ZddNode*& curr_node, ZBDD& current_zdd, int& curr_level,
                      int& stack_pointer, std::vector<bddvar>& nz_zdd_vars);

    void UpdateCurrentNode(ZddNode*& curr_node, ZBDD& current_zdd,
                           int& curr_level);
};
}  // namespace ZDDLSM
