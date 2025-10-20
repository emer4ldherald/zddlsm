#pragma once
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <set>
#include <vector>

#include "../../../SAPPOROBDD/include/ZBDD.h"
#include "compression.h"

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
class LockGuard {
public:
    uint32_t id;

    LockGuard() = delete;

    LockGuard(std::atomic<uint32_t>& curr_task_id,
              std::atomic<uint32_t>& ready_task);

    ~LockGuard();

private:
    std::atomic<uint32_t>& curr_task_id_;
    std::atomic<uint32_t>& ready_task_id_;
};

class Iterator;

class Storage {
public:
    Storage(uint32_t key_len, uint8_t lsm_bits,
            Compression::compression type = Compression::compression::none);

    LockGuard Lock();

    ~Storage() = default;

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
    /*
    Internal representation of a key. Contains column family information.

    Be sure that `key` lifetime is longer that its ZDD internal representation.
    */
    class InternalKey {
    public:
        InternalKey(const std::string& key,
                    const Compression::ICompressor& compressor);

        InternalKey(const std::string& key, uint32_t cf_id,
                    const Compression::ICompressor& compressor);

        const std::string& UserKey() const { return key_; }

        uint32_t CfID() const { return cf_id_; }

        char operator[](uint32_t index) const;

    private:
        const std::string& key_;
        std::string ikey_;
        uint32_t cf_id_;
        uint32_t total_size_;
        bool has_cf_;
    };

    ZBDD store_;
    std::unique_ptr<Compression::ICompressor> compressor_;

    uint32_t key_bit_len_;
    uint8_t lsm_bits_;
    uint32_t bits_for_val_;
    uint32_t max_level_;

    std::atomic<uint32_t> curr_task_id_;
    std::atomic<uint32_t> ready_task_id_;

    std::vector<bddvar> nz_zdd_vars_;

    bool ProcessZddNode(ZBDD& zdd, int& stack_pointer, int top_var_n);

    void GetNzZddVars(const InternalKey& zdd_ikey,
                      uint32_t prefix_len = 0xFFFFFFFF);

    bool AllowsLevel(uint32_t level);

    static inline ZBDD Child(const ZBDD& n, const int child_num);

    inline ZBDD LSMKeyTransform(const InternalKey& key, uint8_t lsm_lev);

    std::optional<ZBDD> GetSubZDDbyKey(const InternalKey& key,
                                       uint32_t prefix_len = 0xFFFFFFFF);

    void InsertImpl(const InternalKey& ikey, uint8_t to_level);

    void UpdateImpl(const InternalKey& ikey, uint8_t from_level,
                    uint8_t to_level);

    void DeleteImpl(const InternalKey& ikey, uint8_t level);

    std::optional<uint8_t> GetLevelImpl(const InternalKey& ikey);

    friend class Iterator;
};

class Iterator {
public:
    Iterator(Storage* zdd, const std::string& key);
    Iterator(Storage* zdd, uint32_t cf_id, const std::string& key);
    Iterator(ZDDLSM::Storage* zdd);
    Iterator(ZDDLSM::Storage* zdd, uint32_t cf_id);

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
    Storage* zdd_;
    bool end_;

    void Init(const std::string& key, ZBDD& initial_zdd);

    bool TraverseNode(ZddNode*& curr_node, ZBDD& current_zdd, int& curr_level,
                      int& stack_pointer, std::vector<bddvar>& nz_zdd_vars);

    void UpdateCurrentNode(ZddNode*& curr_node, ZBDD& current_zdd,
                           int& curr_level);
};
}  // namespace ZDDLSM
