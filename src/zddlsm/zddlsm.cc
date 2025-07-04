#include "include/zddlsm.h"

namespace {

/*
Number of preallocated nodes to avoid extra allocations and rehashing of
SAPPORO.
*/
constexpr static uint32_t ZDD_INIT_SIZE = 2048;

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
ZddInternalKey::ZddInternalKey(const std::string& key)
    : key_(key), cf_id_(0), total_size_(key.size()), has_cf_(false) {}

ZddInternalKey::ZddInternalKey(const std::string& key, uint32_t cf_id)
    : key_(key),
      cf_id_(cf_id),
      total_size_(key.size() + sizeof(cf_id_)),
      has_cf_(true) {}

char ZddInternalKey::operator[](uint32_t index) const {
    if (index >= total_size_) {
        return '\0';
    }
    if (has_cf_ && index < sizeof(cf_id_)) {
        uint32_t byte_pos = sizeof(cf_id_) - index - 1;
        return static_cast<char>((cf_id_ >> byte_pos * 8 * sizeof(char)) &
                                  0x000000FF);
    } else if (has_cf_) {
        return key_[index - sizeof(cf_id_)];
    }
    return key_[index];
}

void Storehouse::GetNzZddVars(const ZDDLSM::ZddInternalKey& zdd_ikey, uint32_t prefix_len) {
    nz_zdd_vars_.clear();
    int nz_bits_n = 0;

    for (uint32_t i = key_bit_len_, j = 0; i != 0; --i, ++j) {
        if (i > prefix_len) {
            continue;
        }
        if (0 != (zdd_ikey[(i - 1) / bits_for_val_] & 1 << j % bits_for_val_)) {
            nz_zdd_vars_.push_back(BDD_LevOfVar(lsm_bits_ + j + 1));
            ++nz_bits_n;
        }
    }

    // std::sort(nz_zdd_vars_.begin(), nz_zdd_vars_.end());
}

bool Storehouse::ProcessZddNode(ZBDD& zdd, int& stack_pointer, int top_var_n) {
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

inline bool Storehouse::AllowsLevel(uint32_t level) {
    return level <= max_level_;
}

inline ZBDD Storehouse::Child(const ZBDD& n, const int child_num) {
    ZBDD g;
    if (child_num != 0)
        g = n.OnSet0(n.Top());
    else
        g = n.OffSet(n.Top());
    return g;
}

inline ZBDD Storehouse::LSMKeyTransform(const ZddInternalKey& zdd_ikey,
                                        uint8_t lsm_lev) {
    ZBDD resulting_zdd = bddsingle;

    // last node stands for msb
    uint8_t lsmBitMask = std::pow(2, lsm_bits_ - 1);
    for (size_t i = 1; i <= lsm_bits_; ++i) {
        if ((lsmBitMask & lsm_lev) != 0) {
            resulting_zdd = resulting_zdd.Change(i);
        }
        lsmBitMask = lsmBitMask >> 1;
    }

    for (size_t i = key_bit_len_, j = 0; i != 0; --i, ++j) {
        if (0 != (zdd_ikey[(i - 1) / bits_for_val_] & 1 << j % bits_for_val_)) {
            resulting_zdd =
                resulting_zdd.Change(lsm_bits_ + (key_bit_len_ - i) + 1);
        }
    }

    return resulting_zdd;
}

std::optional<ZBDD> Storehouse::GetSubZDDbyKey(const ZddInternalKey& key,
                                               uint32_t prefix_len) {
    GetNzZddVars(key, prefix_len);
    ZBDD current_zdd(store_);

    if (isEmpty(current_zdd)) {
        return std::nullopt;
    }

    int stack_pointer = nz_zdd_vars_.size() - 1;
    for (size_t i = 1; i <= key_bit_len_; ++i) {
        auto top_var_n = current_zdd.Top();
        if (isEmpty(current_zdd) || top_var_n <= lsm_bits_ ||
            (prefix_len != 0xFFFFFFFF &&
             static_cast<uint32_t>(top_var_n) <=
                 key_bit_len_ + lsm_bits_ - prefix_len)) {
            break;
        }

        if (!ProcessZddNode(current_zdd, stack_pointer,
                            top_var_n)) {
            return std::nullopt;
        }
    }

    if (stack_pointer < 0 && !isEmpty(current_zdd)) {
        current_zdd = current_zdd.OnSet(current_zdd.Top());
    }

    return (stack_pointer >= 0 || current_zdd == bddfalse)
               ? std::nullopt
               : std::optional<ZBDD>{current_zdd};
}

Storehouse::Storehouse(uint32_t key_bit_len, uint8_t lsm_bits)
    : store_(bddsingle),
      key_bit_len_(key_bit_len + ZDD_ADDITIONAL_BITS),
      lsm_bits_(lsm_bits),
      bits_for_val_(sizeof(char) * 8),
      max_level_(std::pow(2, lsm_bits_) - 1),
      curr_task_id_(0),
      ready_task_id_(0) {
    ZDDSystem::InitOnce(key_bit_len_ + lsm_bits_ + ZDD_ADDITIONAL_BITS);
    nz_zdd_vars_.reserve(key_bit_len_);
}

ZDDLockGuard Storehouse::Lock() {
    return ZDDLockGuard(curr_task_id_, ready_task_id_);
}

void Storehouse::Print() { store_.Print(); }

void Storehouse::InsertImpl(const ZddInternalKey& ikey, uint8_t to_level) {
    ++to_level;
    if(!AllowsLevel(to_level)) {
        return;
    }
    store_ += LSMKeyTransform(ikey, to_level);
}

void Storehouse::Insert(const std::string& key, uint8_t to_level) {
    ZddInternalKey ikey(key);
    InsertImpl(ikey, to_level);
}

void Storehouse::Insert(uint32_t cf_id, const std::string& key, uint8_t to_level) {
    ZddInternalKey ikey(key, cf_id);
    InsertImpl(ikey, to_level);
}

void Storehouse::UpdateImpl(const ZddInternalKey& ikey, uint8_t from_level,
                            uint8_t to_level) {
    ++from_level;
    ++to_level;
    
    if(!AllowsLevel(from_level) || !AllowsLevel(to_level)) {
        return;
    }

    ZBDD transformed_key = LSMKeyTransform(ikey, from_level);
    store_ -= transformed_key;
    uint8_t diff = (from_level) ^ to_level;
    uint8_t lsmBitMask = 1;
    for (size_t i = lsm_bits_; i > 0; --i) {
        if ((diff & lsmBitMask) != 0) {
            transformed_key = transformed_key.Change(i);
        }
        lsmBitMask = lsmBitMask << 1;
    }
    store_ += transformed_key;
}

void Storehouse::Update(const std::string& key, uint8_t from_level,
                        uint8_t to_level) {
    ZddInternalKey ikey(key);
    UpdateImpl(ikey, from_level, to_level);
}

void Storehouse::Update(uint32_t cf_id, const std::string& key,
                        uint8_t from_level, uint8_t to_level) {
    ZddInternalKey ikey(key, cf_id);
    UpdateImpl(ikey, from_level, to_level);
}

void Storehouse::DeleteImpl(const ZddInternalKey& ikey, uint8_t level) {
    ++level;
    store_ -= LSMKeyTransform(ikey, level);
}

void Storehouse::Delete(const std::string& key, uint8_t level) {
    ZddInternalKey ikey(key);
    DeleteImpl(ikey, level);
}

void Storehouse::Delete(uint32_t cf_id, const std::string& key, uint8_t level) {
    ZddInternalKey ikey(key, cf_id);
    DeleteImpl(ikey, level);
}

std::optional<uint8_t> Storehouse::GetLevelImpl(const ZddInternalKey& ikey) {
    std::optional<ZBDD> maybe_subzdd = GetSubZDDbyKey(ikey);

    if (isEmpty() || !maybe_subzdd.has_value() || isEmpty(maybe_subzdd.value())) {
        return std::nullopt;
    }

    ZBDD curr_zdd = maybe_subzdd.value();
    ZBDD current_bit_is_taken;
    ZBDD current_bit_is_not_taken;

    if(BDD_LevOfVar(curr_zdd.Top()) > lsm_bits_) {
        return std::nullopt;
    }

    uint8_t level = 0;

    for (int bit = 0; bit != lsm_bits_; ++bit) {
        current_bit_is_not_taken = Child(curr_zdd, 0);
        current_bit_is_taken = Child(curr_zdd, 1);
        if (current_bit_is_not_taken != bddfalse) {
            curr_zdd = current_bit_is_not_taken;
        } else if (current_bit_is_taken != bddfalse) {
            level = level | (1 << (lsm_bits_ - curr_zdd.Top()));
            curr_zdd = current_bit_is_taken;
        } else {
            return std::nullopt;
        }

        if (curr_zdd == bddtrue) {
            break;
        }
    }

    return level - 1;
}

std::optional<uint8_t> Storehouse::GetLevel(const std::string& key) {
    ZddInternalKey ikey(key);
    return GetLevelImpl(ikey);
}

std::optional<uint8_t> Storehouse::GetLevel(uint32_t cf_id,
                                            const std::string& key) {
    ZddInternalKey ikey(key, cf_id);
    return GetLevelImpl(ikey);
}

bool Storehouse::isEmpty() { return store_ == bddtrue || store_ == bddfalse; }

bool Storehouse::isEmpty(ZBDD store) {
    return store == bddtrue || store == bddfalse;
}

ZDDLockGuard::ZDDLockGuard(std::atomic<uint32_t>& curr_task_id,
                           std::atomic<uint32_t>& ready_task)
    : curr_task_id_(curr_task_id), ready_task_id_(ready_task) {
    id = curr_task_id.fetch_add(1);

    while (ready_task.load() != id) {
    }
}

ZDDLockGuard::~ZDDLockGuard() { ready_task_id_.fetch_add(1); }

bool ZDDLSMIterator::TraverseNode(ZddNode*& curr_node, ZBDD& current_zdd,
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

void ZDDLSMIterator::UpdateCurrentNode(ZddNode*& curr_node, ZBDD& current_zdd,
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

void ZDDLSMIterator::Init(const std::string& key, ZBDD& initial_zdd) {
    nodes_ = std::deque<ZddNode>();
    zdd_->GetNzZddVars(ZddInternalKey(key));

    ZBDD current_zdd(initial_zdd);

    nodes_.push_back({bddnull, BDD_LevOfVar(current_zdd.Top()), false, false});

    if (zdd_->isEmpty(current_zdd)) {
        end_ = true;
        return;
    }

    int stack_pointer = zdd_->nz_zdd_vars_.size() - 1;
    int curr_level = BDD_LevOfVar(current_zdd.Top());
    ZddNode* curr_node = &nodes_.back();

    for (size_t i = 1; i <= zdd_->key_bit_len_; ++i) {
        if (zdd_->isEmpty(current_zdd) ||
            current_zdd.Top() <= zdd_->lsm_bits_) {
            break;
        }

        if (!TraverseNode(curr_node, current_zdd, curr_level, stack_pointer,
                          zdd_->nz_zdd_vars_)) {
            break;
        }
    }

    if (!(stack_pointer >= 0 || current_zdd == bddfalse)) {
        if (!nodes_.empty() && nodes_.back().level <= zdd_->lsm_bits_) {
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

ZDDLSMIterator::ZDDLSMIterator(ZDDLSM::Storehouse* zdd, const std::string& key)
    : zdd_(zdd), end_(false) {
    Init(key, zdd_->store_);
}

ZDDLSMIterator::ZDDLSMIterator(Storehouse* zdd, uint32_t cf_id,
                               const std::string& key)
    : zdd_(zdd), end_(false) {
    ZddInternalKey ikey("", cf_id);
    ZBDD initial_zdd = zdd->GetSubZDDbyKey(ikey, 32).value_or(bddnull);
    if (initial_zdd == bddnull) {
        end_ = true;
    } else {
        Init(key, initial_zdd);
    }
}

ZDDLSMIterator::ZDDLSMIterator(ZDDLSM::Storehouse* zdd)
    : ZDDLSMIterator(zdd, GetMinKey(zdd->key_bit_len_)) {}

ZDDLSMIterator::ZDDLSMIterator(ZDDLSM::Storehouse* zdd, uint32_t cf_id)
    : ZDDLSMIterator(zdd, cf_id, GetMinKey(zdd->key_bit_len_)) {}

void ZDDLSMIterator::Next() {
    if (end_) {
        return;
    }

    ZddNode* curr_node = &nodes_.back();
    int curr_level = curr_node->level;
    ZBDD current_zdd = curr_zdd_;

    if (curr_level <= zdd_->lsm_bits_) {
        nodes_.pop_back();
        current_zdd = curr_node->anc;
        curr_node = &nodes_.back();
        curr_level = curr_node->level;
    }

    while (!nodes_.empty()) {
        if (current_zdd != bddfalse && curr_level <= zdd_->lsm_bits_) {
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

std::optional<KeyLevelPair> ZDDLSMIterator::operator*() const {
    std::string str(zdd_->key_bit_len_ / 8, 0);

    if (end_) {
        return std::nullopt;
    }

    for (ZddNode node : nodes_) {
        int bit_pos = (node.level - zdd_->lsm_bits_ - 1);
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
    str.erase(0, std::distance(str.begin(), it));

    uint8_t level = zdd_->GetLevel(str).value_or(0);

    return KeyLevelPair(std::move(str), level);
}
}  // namespace ZDDLSM
