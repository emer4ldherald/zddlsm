#include "zddlsm/zddlsm.h"

int main() {
    BDD_Init(1024);
    LSMZDD::Storehouse<std::vector<uint32_t>> zdd(64, 3);
    std::cout << "s\n";
    return 0;
}