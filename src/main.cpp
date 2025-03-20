#include "zddlsm/zddlsm.h"
#include <chrono>
#include <fstream>
#include <random>
#include <string>
#include <sys/resource.h>

namespace TEST {
template <typename T> std::vector<T> generateKey(size_t count) {
  std::vector<T> bytes(count);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<T> dist(0, std::pow(2, 8 * sizeof(T)) - 1);

  for (size_t i = 0; i < count; ++i) {
    bytes[i] = static_cast<T>(dist(gen));
  }

  return bytes;
}

double getMemoryUsage() {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);

  return static_cast<double>(usage.ru_maxrss) / 1000;
}

double getTimeInSecs(std::chrono::_V2::system_clock::time_point start) {
  return static_cast<double>(
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now() - start)
                 .count()) /
         1000;
}

void printInfo(LSMZDD::Storehouse<uint32_t> &zdd,
               std::chrono::_V2::system_clock::time_point start) {}

void printResults(uint32_t keyBitLen, uint32_t testSize, uint32_t step,
                  const std::vector<double> &timeSamples,
                  const std::vector<double> &memSamples,
                  const std::string &testsDir) {
  std::string testFile =
      testsDir + std::to_string(keyBitLen) + "_" + std::to_string(testSize);
  std::ofstream f(testFile);

  size_t n = testSize / step + 1;
  f << n << "\n";
  for (size_t i = 0; i < n; ++i) {
    f << i * step << " ";
  }
  f << "\n";
  for (size_t i = 0; i < n; ++i) {
    f << timeSamples[i] << " ";
  }
  f << "\n";
  for (size_t i = 0; i < n; ++i) {
    f << memSamples[i] << " ";
  }
  f.close();
}

void test(uint32_t keyBitLen, uint32_t testSize, const std::string &testsDir) {
  uint32_t keyVecSize = keyBitLen / (sizeof(uint32_t) * 8);

  LSMZDD::Storehouse<uint32_t, std::vector<uint32_t>> zdd(keyBitLen, 4);
  std::vector<uint32_t> key(keyVecSize);

  uint32_t success = 0;
  uint32_t step = 1000;

  auto start = std::chrono::high_resolution_clock::now();

  std::vector<double> timeSamples;
  std::vector<double> memSamples;
  timeSamples.reserve(testSize);
  memSamples.reserve(testSize);

  for (size_t i = 0; i <= testSize; ++i) {
    if (i % step == 0) {
      double memUsed = getMemoryUsage();
      double timeUsed = getTimeInSecs(start);
      zdd.Print();
      std::cout << "Memory used: " << memUsed << "MB\n";
      std::cout << "Time used: " << timeUsed << "s\n\n";
      timeSamples.push_back(timeUsed);
      memSamples.push_back(memUsed);
      start = std::chrono::high_resolution_clock::now();
    }
    key = generateKey<uint32_t>(keyVecSize);
    zdd.Insert(key, 1);
  }

  printResults(keyBitLen, testSize, step, timeSamples, memSamples, testsDir);
}
} // namespace TEST

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cerr << "wrong number of args\n";
    return 1;
  }

  uint32_t keyBitLen = std::stoul(argv[1]);
  uint32_t testSize = std::stoul(argv[2]);

  std::string testsDir = "../../test_res/tests/test_";

  TEST::test(keyBitLen, testSize, testsDir);
  return 0;
}