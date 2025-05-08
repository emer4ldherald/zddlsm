#include "include/zddlsm.h"

std::string AddPadding(const std::string& key, uint32_t keyBitLen) {
    auto key_with_padding = key;
    key_with_padding.reserve(keyBitLen / 8);
    for (size_t i = key.size(); i < (keyBitLen / 8); ++i) {
        key_with_padding.push_back(0);
    }
    return key_with_padding;
}

std::string GetMinKey(size_t bitLen) {
    std::string min_key(bitLen / 8, 0);
    min_key[min_key.size() - 1] = 1;
    return min_key;
}

std::vector<bddvar> GetNzZddVars(const std::string& key, uint32_t keyBitLen,
                                 uint32_t bitsForVal, uint8_t lsmBits) {
    std::vector<bddvar> nz_zdd_vars;
    nz_zdd_vars.reserve(keyBitLen);
    int nz_bits_n = 0;

    // fill var array
    for (uint32_t i = keyBitLen, j = 0; i != 0; --i, ++j) {
        if (0 != (key[(i - 1) / bitsForVal] & 1 << j % bitsForVal)) {
            nz_zdd_vars.push_back(BDD_LevOfVar(lsmBits + j + 1));
            ++nz_bits_n;
        }
    }

    std::sort(nz_zdd_vars.begin(), nz_zdd_vars.end());
    return nz_zdd_vars;
}

namespace ZDDLSM {
bool Storehouse::ProcessZddNode(ZBDD& zdd, std::vector<bddvar>& nz_zdd_vars,
                                int& stack_pointer, int top_var_n) {
    auto level_of_top_var = BDD_LevOfVar(top_var_n);
    if (stack_pointer < 0 || level_of_top_var > nz_zdd_vars[stack_pointer]) {
        zdd = Child(zdd, 0);
    } else if (level_of_top_var < nz_zdd_vars[stack_pointer]) {
        return false;
    } else {
        zdd = Child(zdd, 1);
        --stack_pointer;
    }
    return true;
}

inline ZBDD Storehouse::Child(const ZBDD& n, const int childNum) {
    ZBDD g;
    if (childNum != 0)
        g = n.OnSet0(n.Top());
    else
        g = n.OffSet(n.Top());
    return g;
}

inline ZBDD Storehouse::LSMKeyTransform(const std::string& key, uint32_t keyLen,
                                        uint8_t lsmBits, uint8_t LSMlev,
                                        uint32_t bitsForVal_) {
    ZBDD resulting_zdd = bddsingle;

    // last node stands for msb
    uint8_t lsmBitMask = std::pow(2, lsmBits - 1);
    for (size_t i = 1; i <= lsmBits; ++i) {
        if ((lsmBitMask & LSMlev) != 0) {
            resulting_zdd = resulting_zdd.Change(i);
        }
        lsmBitMask = lsmBitMask >> 1;
    }

    for (size_t i = keyLen, j = 0; i != 0; --i, ++j) {
        if (0 != (key[(i - 1) / bitsForVal_] & 1 << j % bitsForVal_)) {
            resulting_zdd = resulting_zdd.Change(lsmBits + (keyLen - i) + 1);
        }
    }

    return resulting_zdd;
}

std::optional<ZBDD> Storehouse::GetSubZDDbyKey(const std::string& key) {
    std::vector<bddvar> nz_zdd_vars =
        GetNzZddVars(key, keyBitLen, bitsForVal, lsmBits_);
    ZBDD current_zdd(store);

    if (isEmpty(current_zdd)) {
        return std::nullopt;
    }

    int stack_pointer = nz_zdd_vars.size() - 1;
    for (size_t i = 1; i <= keyBitLen; ++i) {
        auto top_var_n = current_zdd.Top();
        if (isEmpty(current_zdd) || top_var_n <= lsmBits_) {
            break;
        }

        if (!ProcessZddNode(current_zdd, nz_zdd_vars, stack_pointer,
                            top_var_n)) {
            return std::nullopt;
        }
    }

    if (stack_pointer < 0 && current_zdd != bddfalse) {
        current_zdd = current_zdd.OnSet(current_zdd.Top());
    }

    return (stack_pointer >= 0 || current_zdd == bddfalse)
               ? std::nullopt
               : std::optional<ZBDD>{current_zdd};
}

bool Storehouse::CheckOnLevel(ZBDD base, uint8_t level) {
    std::vector<bddvar> nz_zdd_vars;
    nz_zdd_vars.reserve(keyBitLen + lsmBits_);
    int nz_bits_n = 0;

    for (size_t i = 1; i <= lsmBits_; ++i) {
        if ((level & (static_cast<uint32_t>(std::pow(2, lsmBits_)) >> i)) !=
            0) {
            nz_zdd_vars.push_back(BDD_LevOfVar(i));
            ++nz_bits_n;
        }
    }

    std::sort(nz_zdd_vars.begin(), nz_zdd_vars.end());

    ZBDD current_zdd(base);

    int stack_pointer = nz_bits_n - 1;
    for (size_t i = 1; i <= lsmBits_; ++i) {
        if (isEmpty(current_zdd)) {
            break;
        }

        if (!ProcessZddNode(current_zdd, nz_zdd_vars, stack_pointer,
                            current_zdd.Top())) {
            return false;
        }
    }

    return stack_pointer < 0 && current_zdd == bddtrue;
}

Storehouse::Storehouse(uint32_t keyLen, uint8_t lsmBits)
    : keyBitLen(keyLen), lsmBits_(lsmBits), bitsForVal(sizeof(char) * 8) {
    BDD_Init(2048);
    for (bddvar v = 1; v <= keyBitLen + lsmBits; ++v) {
        BDD_NewVarOfLev(v);
    }
    store = bddsingle;
}

ZDDLockGuard Storehouse::Lock() { return ZDDLockGuard(); }

void Storehouse::Print() { store.Print(); }

void Storehouse::Insert(const std::string &key, uint8_t level) {
    auto key_with_padding = AddPadding(key, keyBitLen);
    if (level == 1) {
        std::optional<uint32_t> maybe_lev = GetLevel(key_with_padding);
        if (maybe_lev.has_value()) {
            if (maybe_lev.value() == 1) {
                return;
            }
            store -= LSMKeyTransform(key_with_padding, keyBitLen, lsmBits_,
                                     maybe_lev.value(), bitsForVal);
        }
        store += LSMKeyTransform(key_with_padding, keyBitLen, lsmBits_, 1, bitsForVal);
    } else {
        ZBDD transformed_key =
            LSMKeyTransform(key_with_padding, keyBitLen, lsmBits_, level - 1, bitsForVal);
        store -= transformed_key;
        uint8_t diff = (level - 1) ^ level;
        uint8_t lsmBitMask = 1;
        for (size_t i = lsmBits_; i > 0; --i) {
            if ((diff & lsmBitMask) != 0) {
                transformed_key = transformed_key.Change(i);
            }
            lsmBitMask = lsmBitMask << 1;
        }
        store += transformed_key;
    }
}

void Storehouse::Delete(const std::string &key, uint8_t level) {
    auto key_with_padding = AddPadding(key, keyBitLen);
    store -= LSMKeyTransform(key_with_padding, keyBitLen, lsmBits_, level, bitsForVal);
}

std::optional<uint8_t> Storehouse::GetLevel(const std::string &key) {
    auto key_with_padding = AddPadding(key, keyBitLen);
    std::optional<ZBDD> subzdd = GetSubZDDbyKey(key_with_padding);

    if (isEmpty() || !subzdd.has_value()) {
        return std::nullopt;
    }

    for (size_t i = 1; i < std::pow(2, lsmBits_ + 1); ++i) {
        if (CheckOnLevel(subzdd.value(), i)) {
            return std::optional<uint8_t>{i};
        }
    }

    return std::nullopt;
}

std::optional<uint8_t> Storehouse::PushDown(const std::string& key) {
    std::optional<uint8_t> level = GetLevel(key);
    if (!level.has_value()) {
        Insert(key, 1);
        return std::optional<uint8_t>(1);
    } else {
        if (level.value() == std::pow(2, lsmBits_) - 1) {
            return std::nullopt;
        }
        Insert(key, level.value() + 1);
        return std::optional<uint8_t>(level.value() + 1);
    }
}

bool Storehouse::isEmpty() { return store == bddtrue || store == bddfalse; }

bool Storehouse::isEmpty(ZBDD store) {
    return store == bddtrue || store == bddfalse;
}

ZDDLockGuard::ZDDLockGuard() {
    id = currId_.fetch_add(1);

    while (ready_.load() != id) {
    }
}

ZDDLockGuard::~ZDDLockGuard() { ready_.fetch_add(1); }

std::atomic<uint32_t> ZDDLockGuard::currId_{0};
std::atomic<uint32_t> ZDDLockGuard::ready_{0};

bool ZDDLSMIterator::TraverseNode(ZddNode*& curr_node, ZBDD& current_zdd,
                                  int& curr_level, int& stack_pointer,
                                  std::vector<bddvar>& nz_zdd_vars) {
    if (stack_pointer < 0 || curr_level > nz_zdd_vars[stack_pointer]) {
        nodes_.push_back(
            {current_zdd.OnSet(current_zdd.Top()), 0, false, false});

        current_zdd = zdd_->Child(current_zdd, 0);

        curr_level = BDD_LevOfVar(current_zdd.Top());
        curr_node->left = true;
        curr_node = &nodes_.back();
        curr_node->level = curr_level;

    } else if (curr_level < nz_zdd_vars[stack_pointer]) {
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

ZDDLSMIterator::ZDDLSMIterator(ZDDLSM::Storehouse* zdd, const std::string &key)
    : zdd_(zdd), end_(false) {
    auto key_with_padding = AddPadding(key, zdd_->keyBitLen);
    nodes_ = std::deque<ZddNode>();

    std::vector<bddvar> nz_zdd_vars =
        GetNzZddVars(key_with_padding, zdd_->keyBitLen, zdd_->bitsForVal, zdd_->lsmBits_);

    ZBDD current_zdd(zdd_->store);
    nodes_.push_back({bddnull, BDD_LevOfVar(current_zdd.Top()), false, false});

    if (zdd_->isEmpty(current_zdd)) {
        end_ = true;
        return;
    }

    int stack_pointer = nz_zdd_vars.size() - 1;
    int curr_level = BDD_LevOfVar(current_zdd.Top());
    ZddNode* curr_node = &nodes_.back();

    for (size_t i = 1; i <= zdd_->keyBitLen; ++i) {
        if (zdd_->isEmpty(current_zdd) || current_zdd.Top() <= zdd_->lsmBits_) {
            break;
        }

        if (!TraverseNode(curr_node, current_zdd, curr_level, stack_pointer,
                          nz_zdd_vars)) {
            break;
        }
    }

    if (!(stack_pointer >= 0 || current_zdd == bddfalse)) {
        if (!nodes_.empty() && nodes_.back().level <= zdd_->lsmBits_) {
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
        curr_level = curr_node->level;

        curr_zdd_ = current_zdd;

        if (nodes_.empty()) {
            end_ = true;
            return;
        }
    }

    curr_zdd_ = current_zdd;

    Next();
}

ZDDLSMIterator::ZDDLSMIterator(ZDDLSM::Storehouse* zdd)
    : ZDDLSMIterator(zdd, GetMinKey(zdd->keyBitLen)) {}

void ZDDLSMIterator::Next() {
    if (end_) {
        return;
    }

    ZddNode* curr_node = &nodes_.back();
    int curr_level = curr_node->level;
    ZBDD current_zdd = curr_zdd_;

    if (curr_level <= zdd_->lsmBits_) {
        nodes_.pop_back();
        current_zdd = curr_node->anc;
        curr_node = &nodes_.back();
        curr_level = curr_node->level;
    }

    while (!nodes_.empty()) {
        if (current_zdd != bddfalse && curr_level <= zdd_->lsmBits_) {
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
    std::string str(zdd_->keyBitLen / 8, 0);

    if (end_) {
        return std::nullopt;
    }

    for (ZddNode node : nodes_) {
        int bit_pos = (node.level - zdd_->lsmBits_ - 1);
        int char_n = str.size() - bit_pos / 8 - 1;
        str[char_n] = str[char_n] | ((node.right * 1) << bit_pos % 8);
    }

    auto r_it = str.rbegin();
    while (*r_it == 0) {
        str.pop_back();
        r_it = str.rbegin();
    }

    uint8_t level = zdd_->GetLevel(str).value_or(0);

    return KeyLevelPair(std::move(str), level);
}
}  // namespace ZDDLSM