#include "zddlsm/zddlsm.h"
#include <random>
#include <chrono>

template<typename T>
std::vector<T> generateKey(size_t count) {
    std::vector<T> bytes(count);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<T> dist(0, std::pow(2, 8 * sizeof(T)) - 1);
    
    for (size_t i = 0; i < count; ++i) {
        bytes[i] = static_cast<T>(dist(gen));
    }
    
    return bytes;
}

int main() {
    uint32_t keyBitLen = 1024;
    uint32_t keyVecSize = keyBitLen / (sizeof(uint32_t) * 8);
    uint32_t testSize = 100000;
    LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(keyBitLen, 4);
    std::vector<uint32_t> key(keyVecSize);
    uint32_t success = 0;

    for(size_t i = 0; i != testSize; ++i) {
        key = generateKey<uint32_t>(keyVecSize);
        zdd.Insert(key, 1);
        if(i % 1000 == 0) {
            zdd.Print();
            std::cout << i << " pushed, succeed: " << success << "\n";
        }
        if(zdd.GetLevel(key) == 1) {
            ++success;
        } else {
            std::cout << "bad insert\n";
        }
    }
}