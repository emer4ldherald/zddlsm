#include "../SAPPOROBDD/include/ZBDD.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <optional>
#include <set>
#include <vector>

namespace LSMZDD {
template <typename T, typename KeyType = std::vector<T>> class Storehouse {
  ZBDD store;
  uint32_t keyBitLen;
  uint8_t lsmBits_;
  uint32_t bitsForVal;

  static inline ZBDD Child(const ZBDD &n, const int childNum) {
    ZBDD g;
    if (childNum != 0)
      g = n.OnSet0(n.Top());
    else
      g = n.OffSet(n.Top());
    return g;
  }

  static void Count(const ZBDD &n, std::set<KeyType> &v) {
    v.insert(n);
    if (n == bddempty || n == bddsingle || n == bddfalse || n == bddtrue) {
      return;
    }
    Count(Child(n, 0), v);
    Count(Child(n, 1), v);
  }

  static inline ZBDD LSMKeyTransform(const KeyType &key, uint32_t keyLen,
                                     uint8_t lsmBits, uint8_t LSMlev,
                                     uint32_t bitsForVal_) {
    ZBDD blk = bddsingle;

    // last node stands for msb
    uint8_t lsmBitMask = std::pow(2, lsmBits - 1);
    for (size_t i = 1; i <= lsmBits; ++i) {
      if ((lsmBitMask & LSMlev) != 0) {
        blk = blk.Change(i);
      }
      lsmBitMask = lsmBitMask >> 1;
    }

    for (size_t i = 0, j = 0; i != keyLen; ++i, ++j) {
      if (0 != (key[i / bitsForVal_] &
                (static_cast<T>(std::pow(2, bitsForVal_ - 1)) >>
                 (i % bitsForVal_)))) {
        blk = blk.Change(lsmBits + i + 1);
      }
    }

    return blk;
  }

  std::optional<ZBDD> GetSubZDDbyKey(const KeyType &key) {
    std::vector<bddvar> varArray;
    varArray.reserve(keyBitLen);
    int n = 0;

    // fill var array
    for (uint32_t i = 1; i <= keyBitLen; ++i) {
      if (0 != (key[i / bitsForVal + (i % bitsForVal ? 1 : 0) - 1] &
                ((static_cast<T>(std::pow(2, bitsForVal - 1)) >>
                  ((i - 1) % bitsForVal))))) {
        varArray.push_back(BDD_LevOfVar(lsmBits_ + i));
        ++n;
      }
    }

    std::sort(varArray.begin(), varArray.end());

    // for(size_t val : varArray) {
    //   std::cout << val << " ";
    // }
    // std::cout << "\n";

    ZBDD h(store);

    if (isEmpty(h)) {
      return std::nullopt;
    }

    int sp = n - 1;
    for (size_t i = 1; i <= keyBitLen; ++i) {
      // std::cout << i << "\n";
      if (isEmpty(h) || h.Top() <= lsmBits_) {
        // std::cout << "empty: " << isEmpty(h) << "\n";
        break;
      }

      // std::cout << "BDD_LevOfVar(h.Top()) = " << BDD_LevOfVar(h.Top()) <<
      //              ", varArray[sp] = " << varArray[sp] << "\n";

      if (sp < 0 || BDD_LevOfVar(h.Top()) > varArray[sp]) {
        h = Child(h, 0);
      } else if (BDD_LevOfVar(h.Top()) < varArray[sp]) {
        return std::nullopt;
      } else {
        h = Child(h, 1);
        --sp;
      }
    }

    // std::cout << "sp = " << sp << ", false = " << (h == bddfalse) << "\n";

    if (sp < 0 && h != bddfalse) {
      h = h.OnSet(h.Top());
    }

    return (sp >= 0 || h == bddfalse) ? std::nullopt : std::optional<ZBDD>{h};
  }

  bool CheckOnLevel(ZBDD base, uint8_t level) {
    std::vector<bddvar> varArray;
    varArray.reserve(keyBitLen + lsmBits_);
    int n = 0;

    for (size_t i = 1; i <= lsmBits_; ++i) {
      if ((level & (static_cast<uint32_t>(std::pow(2, lsmBits_)) >> i)) != 0) {
        varArray.push_back(BDD_LevOfVar(i));
        ++n;
      }
    }

    std::sort(varArray.begin(), varArray.end());

    ZBDD h(base);
    int sp = n - 1;
    for (size_t i = 1; i <= lsmBits_; ++i) {
      if (isEmpty(h)) {
        break;
      }
      if (sp < 0 || BDD_LevOfVar(h.Top()) > varArray[sp]) {
        h = Child(h, 0);
      } else if (BDD_LevOfVar(h.Top()) < varArray[sp]) {
        return false;
      } else {
        h = Child(h, 1);
        --sp;
      }
    }

    return sp < 0 && h == bddtrue;
  }

public:
  Storehouse(uint32_t keyLen, uint8_t lsmBits)
      : keyBitLen(keyLen), lsmBits_(lsmBits), bitsForVal(sizeof(T) * 8) {
    BDD_Init(2048);
    for (bddvar v = 1; v <= keyBitLen + lsmBits; ++v) {
      BDD_NewVarOfLev(v);
    }
    store = bddsingle;
  }

  ~Storehouse() = default;

  void Print() { store.Print(); }

  uint64_t Size() {
    std::set<ZBDD> visited = {};
    Count(store, visited);
    return visited.size() - 2; // without terminals
  }

  /*
  Inserts `key` on `level`. If `level` = 1, deletes key from its level and
  inserts it on level 1.

  Unsafe: user must ensure that `key` is placed on `level - 1` before calling,
  otherwise UB.
  */
  void Insert(const KeyType &key, uint8_t level) {
    if (level == 1) {
      std::optional<uint32_t> maybe_lev = GetLevel(key);
      if (maybe_lev.has_value()) {
        if (maybe_lev.value() == 1) {
          return;
        }
        store -= LSMKeyTransform(key, keyBitLen, lsmBits_, maybe_lev.value(),
                                 bitsForVal);
      }
      store += LSMKeyTransform(key, keyBitLen, lsmBits_, 1, bitsForVal);
    } else {
      ZBDD transformed_key =
          LSMKeyTransform(key, keyBitLen, lsmBits_, level - 1, bitsForVal);
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

  /*
  Deletes `key` placed on `level`
  */
  void Delete(const KeyType &key, uint8_t level) {
    store -= LSMKeyTransform(key, keyBitLen, lsmBits_, level, bitsForVal);
  }

  /*
  Returns `std::optional` of current `level` of `key` or `std::nullopt` if
  there's not `key` in zdd.
  */
  std::optional<uint8_t> GetLevel(const KeyType &key) {
    std::optional<ZBDD> subzdd = GetSubZDDbyKey(key);

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

  /*
  For `key` changes its level to (level + 1). Inserts on level `1` if there's
  not `key` in zdd.

  Returns `std::optional` of resulting `key`. If `key` is already on the topmost
  level, returns `std::nullopt`.
  */
  std::optional<uint8_t> PushDown(const KeyType &key) {
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

  bool isEmpty() { return store == bddtrue || store == bddfalse; }

  static bool isEmpty(ZBDD store) {
    return store == bddtrue || store == bddfalse;
  }
};
} // namespace LSMZDD
