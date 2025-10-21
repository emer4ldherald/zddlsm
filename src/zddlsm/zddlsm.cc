#include "include/zddlsm.h"

namespace {

/*
Number of preallocated nodes to avoid extra allocations and rehashing of
SAPPORO.
*/
constexpr static uint32_t ZDD_INIT_SIZE = 4096;
constexpr static uint32_t BITS_IN_BYTE = 8;
constexpr static int DATA_BIT_LEN = sizeof(uint32_t) * BITS_IN_BYTE;
constexpr static int BITS_FOR_VAL = sizeof(char) * BITS_IN_BYTE;
constexpr static int SHARDS_DEFAULT_NUMBER = 1000;

/*
Bits for column family information and for other purposes.
*/
constexpr static uint32_t ZDD_ADDITIONAL_BITS = 32;

std::string GetMinKey(size_t key_bit_len) {
    std::string min_key(key_bit_len / 8, 0);
    min_key[min_key.size() - 1] = 1;
    return min_key;
}

/*
Singleton object initilizes ZDD.
*/
class ZDDSystem {
public:
    static void InitOnce(uint32_t total_vars) {
        static ZDDSystem instance(total_vars);
    }

private:
    ZDDSystem(uint32_t total_vars) {
        BDD_Init(ZDD_INIT_SIZE);
        for (bddvar v = 1; v <= total_vars; ++v) {
            BDD_NewVarOfLev(v);
        }
    }
};
}  // namespace

namespace ZDDLSM {

Storage::InternalKey::InternalKey(const std::string& key,
                                  const Compression::ICompressor& compressor)
    : key_(key), cf_id_(0), has_cf_(false) {
    ikey_ = compressor.Compress(key);
    total_size_ = ikey_.size();
}

Storage::InternalKey::InternalKey(const std::string& key, uint32_t cf_id,
                                  const Compression::ICompressor& compressor)
    : key_(key), cf_id_(cf_id), has_cf_(true) {
    ikey_ = compressor.Compress(key);
    total_size_ = ikey_.size() + sizeof(cf_id_);
}

char Storage::InternalKey::operator[](uint32_t index) const {
    if (index >= total_size_) {
        return 0;
    }
    if (has_cf_ && index < sizeof(cf_id_)) {
        uint32_t byte_pos = sizeof(cf_id_) - index - 1;
        return static_cast<char>((cf_id_ >> byte_pos * 8 * sizeof(char)) &
                                 0x000000FF);
    } else if (has_cf_) {
        return ikey_[index - sizeof(cf_id_)];
    }
    return ikey_[index];
}

void Storage::GetNzZddVars(const InternalKey& zdd_ikey, uint32_t prefix_len) {
    nz_zdd_vars_.clear();
    int nz_bits_n = 0;

    for (uint32_t i = std::min(key_bit_len_, prefix_len), j = key_bit_len_ - i;
         i != 0; --i, ++j) {
        if (0 != (zdd_ikey[(i - 1) / BITS_FOR_VAL] & 1 << j % BITS_FOR_VAL)) {
            nz_zdd_vars_.push_back(BDD_LevOfVar(DATA_BIT_LEN + j + 1));
            ++nz_bits_n;
        }
    }

    if (!std::is_sorted(nz_zdd_vars_.begin(), nz_zdd_vars_.end())) {
        std::sort(nz_zdd_vars_.begin(), nz_zdd_vars_.end());
    }
}

bool Storage::ProcessZddNode(ZBDD& zdd, int& stack_pointer, int top_var_n) {
    auto level_of_top_var = BDD_LevOfVar(top_var_n);
    if (stack_pointer < 0 ||
        level_of_top_var > static_cast<int64_t>(nz_zdd_vars_[stack_pointer])) {
        zdd = Child(zdd, 0);
    } else if (level_of_top_var <
               static_cast<int64_t>(nz_zdd_vars_[stack_pointer])) {
        return false;
    } else {
        zdd = Child(zdd, 1);
        --stack_pointer;
    }
    return true;
}

inline ZBDD Storage::Child(const ZBDD& n, const int child_num) {
    ZBDD g;
    if (child_num != 0)
        g = n.OnSet0(n.Top());
    else
        g = n.OffSet(n.Top());
    return g;
}

inline ZBDD Storage::LSMKeyTransform(const InternalKey& zdd_ikey,
                                     uint32_t lsm_lev) {
    ZBDD resulting_zdd = bddsingle;

    // last node stands for msb
    uint32_t bit_mask = std::pow(2, DATA_BIT_LEN - 1);
    for (size_t i = 1; i <= DATA_BIT_LEN; ++i) {
        if ((bit_mask & lsm_lev) != 0) {
            resulting_zdd = resulting_zdd.Change(i);
        }
        bit_mask = bit_mask >> 1;
    }

    for (size_t i = key_bit_len_, j = 0; i != 0; --i, ++j) {
        if (0 != (zdd_ikey[(i - 1) / BITS_FOR_VAL] & 1 << j % BITS_FOR_VAL)) {
            resulting_zdd =
                resulting_zdd.Change(DATA_BIT_LEN + (key_bit_len_ - i) + 1);
        }
    }

    return resulting_zdd;
}

std::optional<ZBDD> Storage::GetSubZDDbyKey(const InternalKey& key,
                                            uint32_t prefix_len) {
    GetNzZddVars(key, prefix_len);
    ZBDD current_zdd(store_);

    if (IsEmpty(current_zdd)) {
        return std::nullopt;
    }

    int stack_pointer = nz_zdd_vars_.size() - 1;
    for (size_t i = 1; i <= key_bit_len_; ++i) {
        auto top_var_n = current_zdd.Top();
        if (IsEmpty(current_zdd) || top_var_n <= DATA_BIT_LEN ||
            (prefix_len != 0xFFFFFFFF &&
             static_cast<uint32_t>(top_var_n) <=
                 key_bit_len_ + DATA_BIT_LEN - prefix_len)) {
            break;
        }

        if (!ProcessZddNode(current_zdd, stack_pointer, top_var_n)) {
            return std::nullopt;
        }
    }

    if (stack_pointer < 0 && !IsEmpty(current_zdd)) {
        current_zdd = current_zdd.OnSet(current_zdd.Top());
    }

    return (stack_pointer >= 0 || current_zdd == bddfalse)
               ? std::nullopt
               : std::optional<ZBDD>{current_zdd};
}

Storage::Storage(uint32_t key_len, Compression::compression type)
    : store_(bddsingle),
      current_token_(0),
      size_(0),
      deleted_(0),
      curr_task_id_(0),
      ready_task_id_(0) {
    compressor_ = Compression::BuildCompressor(type);
    key_bit_len_ = compressor_->BytesNeeds(key_len) * 8 + ZDD_ADDITIONAL_BITS;
    ZDDSystem::InitOnce(key_bit_len_ + DATA_BIT_LEN);
    nz_zdd_vars_.reserve(key_bit_len_);
}

LockGuard Storage::Lock() { return LockGuard(curr_task_id_, ready_task_id_); }

void Storage::Print() { store_.Print(); }

void Storage::SetImpl(const InternalKey& ikey, uint32_t to_level) {
    std::optional<uint32_t> level_key = GetLevelImpl(ikey);
    if (level_key.has_value()) {
        data_[level_key.value()] = to_level;
    } else {
        store_ += LSMKeyTransform(ikey, ++current_token_);
        ++size_;
        data_[current_token_] = to_level;
    }
}

void Storage::Set(const std::string& key, uint32_t to_level) {
    InternalKey ikey(key, *compressor_);
    SetImpl(ikey, to_level);
}

void Storage::Set(uint32_t cf_id, const std::string& key, uint32_t to_level) {
    InternalKey ikey(key, cf_id, *compressor_);
    SetImpl(ikey, to_level);
}


void Storage::SetNoCompr(uint32_t cf_id, const std::string& key, uint32_t to_level) {
    InternalKey ikey(key, cf_id, Compression::NoCompression());
    SetImpl(ikey, to_level);
}

void Storage::SetNoCompr(const std::string& key, uint32_t to_level) {
    InternalKey ikey(key, Compression::NoCompression());
    SetImpl(ikey, to_level);
}


void Storage::DeleteImpl(const InternalKey& ikey) {
    std::optional<uint32_t> level_key = GetLevelImpl(ikey);
    if (level_key.has_value()) {
        data_.erase(level_key.value());
        store_ -= LSMKeyTransform(ikey, level_key.value());
        --size_;
        ++deleted_;
    }
}

void Storage::Delete(const std::string& key) {
    InternalKey ikey(key, *compressor_);
    DeleteImpl(ikey);
}

void Storage::Delete(uint32_t cf_id, const std::string& key) {
    InternalKey ikey(key, cf_id, *compressor_);
    DeleteImpl(ikey);
}

std::optional<uint32_t> Storage::GetLevelImpl(const InternalKey& ikey) {
    std::optional<ZBDD> maybe_subzdd = GetSubZDDbyKey(ikey);

    if (IsEmpty() || !maybe_subzdd.has_value() ||
        IsEmpty(maybe_subzdd.value())) {
        return std::nullopt;
    }

    ZBDD curr_zdd = maybe_subzdd.value();
    ZBDD current_bit_is_taken;
    ZBDD current_bit_is_not_taken;

    if (BDD_LevOfVar(curr_zdd.Top()) > DATA_BIT_LEN) {
        return std::nullopt;
    }

    uint32_t data_key_ = 0;

    for (int bit = 0; bit != DATA_BIT_LEN; ++bit) {
        current_bit_is_not_taken = Child(curr_zdd, 0);
        current_bit_is_taken = Child(curr_zdd, 1);
        if (current_bit_is_not_taken != bddfalse) {
            curr_zdd = current_bit_is_not_taken;
        } else if (current_bit_is_taken != bddfalse) {
            data_key_ = data_key_ | (1 << (DATA_BIT_LEN - curr_zdd.Top()));
            curr_zdd = current_bit_is_taken;
        } else {
            return std::nullopt;
        }

        if (curr_zdd == bddtrue) {
            break;
        }
    }

    return data_key_;
}

std::optional<uint32_t> Storage::GetLevel(const std::string& key) {
    InternalKey ikey(key, *compressor_);
    std::optional<uint32_t> level_key = GetLevelImpl(ikey);
    if (level_key.has_value()) {
        return data_[level_key.value()];
    }
    return std::nullopt;
}

std::optional<uint32_t> Storage::GetLevel(uint32_t cf_id,
                                          const std::string& key) {
    InternalKey ikey(key, cf_id, *compressor_);
    std::optional<uint32_t> level_key = GetLevelImpl(ikey);
    if (level_key.has_value()) {
        return data_[level_key.value()];
    }
    return std::nullopt;
}

std::optional<uint32_t> Storage::GetLevelNoCompr(const std::string& key) {
    InternalKey ikey(key, Compression::NoCompression());
    std::optional<uint32_t> level_key = GetLevelImpl(ikey);
    if (level_key.has_value()) {
        return data_[level_key.value()];
    }
    return std::nullopt;
}

std::optional<uint32_t> Storage::GetLevelNoCompr(uint32_t cf_id, const std::string& key) {
    InternalKey ikey(key, cf_id, Compression::NoCompression());
    std::optional<uint32_t> level_key = GetLevelImpl(ikey);
    if (level_key.has_value()) {
        return data_[level_key.value()];
    }
    return std::nullopt;
}

bool Storage::IsEmpty() { return store_ == bddtrue || store_ == bddfalse; }

bool Storage::IsEmpty(ZBDD store) {
    return store == bddtrue || store == bddfalse;
}

LockGuard::LockGuard(std::atomic<uint32_t>& curr_task_id,
                     std::atomic<uint32_t>& ready_task)
    : curr_task_id_(curr_task_id), ready_task_id_(ready_task) {
    id = curr_task_id.fetch_add(1);

    while (ready_task.load() != id) {
    }
}

LockGuard::~LockGuard() { ready_task_id_.fetch_add(1); }

bool Iterator::TraverseNode(ZddNode*& curr_node, ZBDD& current_zdd,
                            int& curr_level, int& stack_pointer,
                            std::vector<bddvar>& nz_zdd_vars) {
    if (stack_pointer < 0 ||
        curr_level > static_cast<int64_t>(nz_zdd_vars[stack_pointer])) {
        nodes_.push_back(
            {current_zdd.OnSet(current_zdd.Top()), 0, false, false});

        current_zdd = zdd_->Child(current_zdd, 0);

        curr_level = BDD_LevOfVar(current_zdd.Top());
        curr_node->left = true;
        curr_node = &nodes_.back();
        curr_node->level = curr_level;

    } else if (curr_level < static_cast<int64_t>(nz_zdd_vars[stack_pointer])) {
        curr_node = &nodes_.back();
        curr_level = curr_node->level;
        nodes_.pop_back();
        return false;

    } else {
        nodes_.push_back(
            {current_zdd.OnSet(current_zdd.Top()), 0, false, false});

        current_zdd = zdd_->Child(current_zdd, 1);

        curr_level = BDD_LevOfVar(current_zdd.Top());
        curr_node->left = true;
        curr_node->right = true;
        curr_node = &nodes_.back();
        curr_node->level = curr_level;

        --stack_pointer;
    }
    return true;
}

ShardedStorage::ShardedStorage(uint32_t key_size)
    : ShardedStorage (key_size, Compression::compression::none, SHARDS_DEFAULT_NUMBER) {}

ShardedStorage::ShardedStorage(uint32_t key_size, uint32_t shards_number)
    : ShardedStorage (key_size, Compression::compression::none, shards_number) {}

ShardedStorage::ShardedStorage(uint32_t key_size, Compression::compression type) 
    : ShardedStorage(key_size, type, SHARDS_DEFAULT_NUMBER) {}

ShardedStorage::ShardedStorage(uint32_t key_size, Compression::compression type, uint32_t shards_number)
    : key_size_(key_size),
      max_votes_for_gc_(shards_number / 10),
      votes_for_gc_(0), c_type_(type) {
    for (uint32_t i = 0; i != shards_number; ++i) {
        shards_.push_back(std::make_unique<ZDDLSM::Storage>(key_size, type));
    }
}

void ShardedStorage::Print() const {
    for (uint32_t i = 0; i != shards_.size(); ++i) {
        std::cout << "shard " << i << " -- size: " << shards_[i]->Size()
                  << ", deleted: " << shards_[i]->Deleted() << "\n";
    }
}

void ShardedStorage::VoteForGC() {
    votes_for_gc_.fetch_add(1);

    int max_votes = max_votes_for_gc_;

    if (votes_for_gc_.compare_exchange_strong(max_votes, 0)) {
        BDD_GC();
    }
}

uint32_t ShardedStorage::GetShardPos(const std::string& key) const {
    return std::hash<std::string>{}(key) % shards_.size();
}

void ShardedStorage::Cleanup(uint32_t shard_pos) {
    // lock acquired
    if (shards_[shard_pos]->Deleted() < (100000 / shards_.size()) / 3) {
        return;
    }

    auto it = Iterator(shards_[shard_pos].get());
    auto clean_storage = std::make_unique<ZDDLSM::Storage>(key_size_, c_type_);

    while (it.HasNext()) {
        KeyLevelPair kl = (*it).value();
        clean_storage->SetNoCompr(kl.Key(), kl.Level());
        it.Next();
    }

    if (shards_[shard_pos]->Size() != clean_storage->Size()) {
        std::cerr << "sizes not equal. old: " << shards_[shard_pos]->Size()
                  << ", new: " << clean_storage->Size() << "\n";
    }

    shards_[shard_pos].reset();
    shards_[shard_pos] = std::move(clean_storage);

    VoteForGC();
}

void ShardedStorage::Set(const std::string& key, uint32_t to_level) {
    uint32_t shard_pos = GetShardPos(key);

    auto lock_guard = shards_[shard_pos]->Lock();
    shards_[shard_pos]->Set(key, to_level);
}

void ShardedStorage::Set(uint32_t cf_id, const std::string& key,
                         uint32_t to_level) {
    uint32_t shard_pos = GetShardPos(key);

    auto lock_guard = shards_[shard_pos]->Lock();
    shards_[shard_pos]->Set(cf_id, key, to_level);
}

void ShardedStorage::Delete(const std::string& key) {
    uint32_t shard_pos = GetShardPos(key);

    auto lock_guard = shards_[shard_pos]->Lock();
    shards_[shard_pos]->Delete(key);
    Cleanup(shard_pos);
}

void ShardedStorage::Delete(uint32_t cf_id, const std::string& key) {
    uint32_t shard_pos = GetShardPos(key);

    auto lock_guard = shards_[shard_pos]->Lock();
    shards_[shard_pos]->Delete(cf_id, key);
    Cleanup(shard_pos);
}

std::optional<uint32_t> ShardedStorage::GetLevel(const std::string& key) {
    uint32_t shard_pos = GetShardPos(key);

    auto lock_guard = shards_[shard_pos]->Lock();
    return shards_[shard_pos]->GetLevel(key);
}

std::optional<uint32_t> ShardedStorage::GetLevel(uint32_t cf_id,
                                                 const std::string& key) {
    uint32_t shard_pos = GetShardPos(key);

    auto lock_guard = shards_[shard_pos]->Lock();
    return shards_[shard_pos]->GetLevel(cf_id, key);
}

void Iterator::UpdateCurrentNode(ZddNode*& curr_node, ZBDD& current_zdd,
                                 int& curr_level) {
    if (current_zdd != bddfalse) {
        curr_level = BDD_LevOfVar(current_zdd.Top());
        curr_node = &nodes_.back();
        curr_node->level = curr_level;
    } else {
        current_zdd = nodes_.back().anc;
        nodes_.pop_back();
        curr_node = &nodes_.back();
        curr_level = curr_node->level;
    }
}

void Iterator::Init(const std::string& key, ZBDD& initial_zdd) {
    nodes_ = std::deque<ZddNode>();
    zdd_->GetNzZddVars(
        Storage::InternalKey(key, Compression::NoCompression())
    );

    ZBDD current_zdd(initial_zdd);

    nodes_.push_back({bddnull, BDD_LevOfVar(current_zdd.Top()), false, false});

    if (zdd_->IsEmpty(current_zdd)) {
        end_ = true;
        return;
    }

    int stack_pointer = zdd_->nz_zdd_vars_.size() - 1;
    int curr_level = BDD_LevOfVar(current_zdd.Top());
    ZddNode* curr_node = &nodes_.back();

    for (size_t i = 1; i <= zdd_->key_bit_len_; ++i) {
        if (zdd_->IsEmpty(current_zdd) || current_zdd.Top() <= DATA_BIT_LEN) {
            break;
        }

        if (!TraverseNode(curr_node, current_zdd, curr_level, stack_pointer,
                          zdd_->nz_zdd_vars_)) {
            break;
        }
    }

    if (!(stack_pointer >= 0 || current_zdd == bddfalse)) {
        if (!nodes_.empty() && nodes_.back().level <= DATA_BIT_LEN) {
            nodes_.pop_back();
        } else if (nodes_.empty()) {
            end_ = true;
        }
        return;
    }

    if (current_zdd == bddfalse) {
        if (nodes_.empty()) {
            end_ = true;
            return;
        }
        nodes_.pop_back();
        current_zdd = curr_node->anc;
        curr_node = &nodes_.back();

        curr_zdd_ = current_zdd;

        if (nodes_.empty()) {
            end_ = true;
            return;
        }
    }

    curr_zdd_ = current_zdd;

    Next();
}

Iterator::Iterator(ZDDLSM::Storage* zdd, const std::string& key)
    : zdd_(zdd), end_(false) {
    Init(key, zdd_->store_);
}

Iterator::Iterator(Storage* zdd, uint32_t cf_id, const std::string& key)
    : zdd_(zdd), end_(false) {
    Storage::InternalKey ikey("", cf_id, Compression::NoCompression());
    ZBDD initial_zdd = zdd->GetSubZDDbyKey(ikey, 32).value_or(bddnull);
    if (initial_zdd == bddnull) {
        end_ = true;
    } else {
        Init(key, initial_zdd);
    }
}

Iterator::Iterator(ZDDLSM::Storage* zdd)
    : Iterator(zdd, GetMinKey(zdd->key_bit_len_)) {}

Iterator::Iterator(ZDDLSM::Storage* zdd, uint32_t cf_id)
    : Iterator(zdd, cf_id, GetMinKey(zdd->key_bit_len_)) {}

bool Iterator::HasNext() const { return !end_; }

void Iterator::Next() {
    if (end_) {
        return;
    }

    ZddNode* curr_node = &nodes_.back();
    int curr_level = curr_node->level;
    ZBDD current_zdd = curr_zdd_;

    if (curr_level <= DATA_BIT_LEN) {
        nodes_.pop_back();
        current_zdd = curr_node->anc;
        curr_node = &nodes_.back();
        curr_level = curr_node->level;
    }

    while (!nodes_.empty()) {
        if (current_zdd != bddfalse && curr_level <= DATA_BIT_LEN) {
            break;
        }

        if (current_zdd == bddfalse) {
            current_zdd = curr_node->anc;
            curr_node = &nodes_.back();
            curr_level = curr_node->level;
            continue;
        }

        if (!curr_node->left && !curr_node->right) {
            nodes_.push_back(
                {current_zdd.OnSet(current_zdd.Top()), 0, false, false});
            current_zdd = zdd_->Child(current_zdd, 0);
            curr_node->left = true;

            UpdateCurrentNode(curr_node, current_zdd, curr_level);
        } else if (curr_node->left && !curr_node->right) {
            nodes_.push_back(
                {current_zdd.OnSet(current_zdd.Top()), 0, false, false});
            current_zdd = zdd_->Child(current_zdd, 1);
            curr_node->right = true;

            UpdateCurrentNode(curr_node, current_zdd, curr_level);
        } else if (curr_node->left && curr_node->right) {
            if (nodes_.empty()) {
                break;
            }

            current_zdd = nodes_.back().anc;
            nodes_.pop_back();
            if (!nodes_.empty()) {
                curr_node = &nodes_.back();
                curr_level = curr_node->level;
            }
            if (current_zdd == bddfalse) {
                break;
            }
        }

        curr_zdd_ = current_zdd;
    }
    if (nodes_.empty()) {
        end_ = true;
        return;
    }

    nodes_.pop_back();
}

std::optional<KeyLevelPair> Iterator::operator*() const {
    std::string str(zdd_->key_bit_len_ / 8, 0);

    if (end_) {
        return std::nullopt;
    }

    for (ZddNode node : nodes_) {
        int bit_pos = (node.level - DATA_BIT_LEN - 1);
        int char_n = str.size() - bit_pos / 8 - 1;
        str[char_n] = str[char_n] | ((node.right * 1) << bit_pos % 8);
    }

    auto r_it = str.rbegin();
    while (*r_it == 0) {
        ++r_it;
    }
    str.resize(str.size() - std::distance(str.rbegin(), r_it));

    auto it = str.begin();
    while (*it == 0) {
        ++it;
    }
    //str.erase(0, std::distance(str.begin(), it));

    uint32_t level = zdd_->GetLevelNoCompr(str).value_or(0);

    return KeyLevelPair(std::move(str), level);
}
}  // namespace ZDDLSM
