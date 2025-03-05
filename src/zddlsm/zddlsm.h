#include <algorithm>
#include <cmath>
#include <cstdint>
#include <optional>
#include <set>
#include <vector>

#include "../SAPPOROBDD/SAPPOROBDD/include/ZBDD.h"

namespace LSMZDD {
template <typename KeyType = std::vector<uint32_t>> class Storehouse {
  ZBDD store;
  uint32_t keyBitLen;
  uint8_t lsmBits_;

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

  static inline ZBDD KeyTransform(const KeyType &key, uint32_t keyLen) {
    ZBDD blk = bddsingle;
    for (size_t i = keyLen, j = 0; i > 0; --i, ++j) {
      if (0 != (key[i / 32 + (i % 32 ? 1 : 0) - 1] & (1 << (j % 32)))) {
        blk = blk.Change(i);
      }
    }
    return blk;
  }

  static inline ZBDD LSMKeyTransform(const KeyType &key, uint32_t keyLen,
                                     uint8_t lsmBits, uint8_t LSMlev) {
    ZBDD blk = bddsingle;

    // last node stands for msb
    for (size_t i = keyLen, j = 0; i > 0; --i, ++j) {
      if (0 != (key[i / 32 + (i % 32 ? 1 : 0) - 1] & (1 << (j % 32)))) {
        blk = blk.Change(lsmBits + i);
      }
    }

    uint8_t lsmBitMask = 1;
    for (size_t i = lsmBits; i > 0; --i) {
      if ((lsmBitMask & LSMlev) != 0) {
        blk = blk.Change(i);
      }
      lsmBitMask = lsmBitMask << 1;
    }

    return blk;
  }

public:
  Storehouse(uint32_t keyLen, uint8_t lsmBits)
      : keyBitLen(keyLen), lsmBits_(lsmBits) {
    BDD_Init(1024);
    for (bddvar v = 1; v <= keyBitLen + lsmBits; ++v) {
      BDD_NewVarOfLev(v);
    }
    store = bddsingle;
  }

  ~Storehouse() = default;

  uint64_t Size() {
    std::set<ZBDD> visited = {};
    Count(store, visited);
    return visited.size() - 2; // without terminals
  }

  void Insert(const KeyType &key) { store += KeyTransform(key, keyBitLen); }

  void Delete(const KeyType &key) { store -= KeyTransform(key, keyBitLen); }

  void LSMinsert(const KeyType &key, uint8_t level) {
    // one keyTransform while incrementing?
    //  if(level > 1) {
    //      store -= LSMKeyTransform(key, keyBitLen, lsmBits_, level - 1);
    //  }

    // store += LSMKeyTransform(key, keyBitLen, lsmBits_, level);

    if (level == 1) {
      store += LSMKeyTransform(key, keyBitLen, lsmBits_, 1);
    } else {
      ZBDD transformed_key =
          LSMKeyTransform(key, keyBitLen, lsmBits_, level - 1);
      store -= transformed_key;
      uint8_t diff = (level - 1) ^ level;
      uint8_t lsmBitMask = 1;
      for (size_t i = lsmBits_; i > 0; --i) {
        if ((diff & lsmBitMask) != 0) {
          std::cout << (lsmBitMask & lsmBitMask) << "\n";
          transformed_key = transformed_key.Change(i);
        }
        lsmBitMask = lsmBitMask << 1;
      }
      store += transformed_key;
    }
  }

  void LSMdelete(const KeyType &key, uint8_t level) {
    store -= LSMKeyTransform(key, keyBitLen, lsmBits_, level);
  }

  std::optional<ZBDD> GetSubZDDbyKey(const KeyType &key) {
    std::vector<bddvar> varArray;
    varArray.reserve(keyBitLen + lsmBits_);
    int n = 0;

    // fill var array
    for (uint32_t i = 1; i <= keyBitLen; ++i) {
      if (0 != (key[i / 32 + (i % 32 ? 1 : 0) - 1] &
                ((static_cast<uint32_t>(std::pow(2, 31)) >> ((i - 1) % 32))))) {
        varArray.push_back(BDD_LevOfVar(lsmBits_ + i));
        ++n;
      }
    }

    std::sort(varArray.begin(), varArray.end());

    ZBDD h(store);
    int sp = n - 1;
    for (size_t i = 1; i <= keyBitLen; ++i) {
      if (isEmpty(h) || h.Top() <= lsmBits_) {
        break;
      }
      if (sp < 0 || BDD_LevOfVar(h.Top()) > varArray[sp]) {
        h = Child(h, 0);
      } else if (BDD_LevOfVar(h.Top()) < varArray[sp]) {
        return std::nullopt;
      } else {
        h = Child(h, 1);
        --sp;
      }
    }

    if (h != bddfalse) {
      h = h.OnSet(h.Top());
    }

    return (sp >= 0 || h == bddfalse) ? std::nullopt : std::optional<ZBDD>{h};
  }

  bool checkOnLevel(ZBDD base, uint8_t level) {
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

  std::optional<uint8_t> getLevel(const KeyType &key) {
    std::optional<ZBDD> subzdd = GetSubZDDbyKey(key);

    if (!subzdd.has_value()) {
      return std::nullopt;
    }

    for (size_t i = 1; i < std::pow(2, lsmBits_ + 1); ++i) {
      if (checkOnLevel(subzdd.value(), i)) {
        return std::optional<uint8_t>{i};
      }
    }

    return std::nullopt;
  }

  size_t getTop() { return store.Top(); }

  bool isEmpty() { return store == bddtrue || store == bddfalse; }

  static bool isEmpty(ZBDD store) {
    return store == bddtrue || store == bddfalse;
  }
};
} // namespace LSMZDD
