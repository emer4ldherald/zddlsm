#include <sys/resource.h>

#include <chrono>
#include <fstream>
#include <random>
#include <string>

#include "zddlsm/include/zddlsm.h"

namespace TEST {
std::string GenerateKey(size_t count) {
    std::string bytes(count, 0);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(32, 126);

    for (size_t i = 0; i < count; ++i) {
        bytes[i] = static_cast<char>(dist(gen));
    }

    return bytes;
}

double GetMemoryUsage() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);

    return static_cast<double>(usage.ru_maxrss) / 1000;

    std::ifstream status_file("/proc/self/status");
    std::string line;

    while (std::getline(status_file, line)) {
        if (line.starts_with("VmRSS:")) {
            size_t pos = line.find(':');
            size_t kb = std::stoul(line.substr(pos + 1));
            return kb / 1000;
        }
    }
    return 0;
}

double GetTimeInSecs(std::chrono::_V2::system_clock::time_point start) {
    return static_cast<double>(
               std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()) /
           1000;
}

void PrintResults(uint32_t test_size, Compression::compression type,
                  uint32_t step, const std::vector<double>& time_samples,
                  const std::vector<double>& mem_samples,
                  const std::string& tests_dir, const std::string& test_name) {
    auto compression = [type]() {
        switch (type) {
            case Compression::compression::zstd:
                return "zstd";
            case Compression::compression::md5:
                return "md5";
            case Compression::compression::sha256:
                return "sha256";
            default:
                return "uncompressed";
        }
    };

    std::ofstream f(tests_dir + test_name + "_" + compression() + ".out");

    size_t n = test_size / step + 1;
    f << n << "\n";
    for (size_t i = 0; i < n; ++i) {
        f << i * step << " ";
    }
    f << "\n";
    for (size_t i = 0; i < n; ++i) {
        f << time_samples[i] << " ";
    }
    f << "\n";
    for (size_t i = 0; i < n; ++i) {
        f << mem_samples[i] << " ";
    }
    f.close();
}

void test(uint32_t key_byte_len, uint32_t test_size,
          Compression::compression type, const std::string& tests_dir,
          const std::string& test_name) {
    ZDDLSM::Storage zdd(key_byte_len * 8, type);

    uint32_t step = 1000;

    std::vector<double> time_samples;
    std::vector<double> mem_samples;
    time_samples.reserve(test_size);
    mem_samples.reserve(test_size);

    auto compression = [type]() {
        switch (type) {
            case Compression::compression::zstd:
                return "zstd";
            case Compression::compression::md5:
                return "md5";
            case Compression::compression::sha256:
                return "sha256";
            default:
                return "uncompressed";
        }
    };

    std::fstream testfile(tests_dir + test_name);
    std::string key;
    std::getline(testfile, key);

    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i <= test_size; ++i) {
        if (i % step == 0) {
            double mem_used = GetMemoryUsage();
            double time_used = GetTimeInSecs(start);
            std::cout << "Compression type: " << compression() << "\n";
            std::cout << "Inserted        : " << i << "\n";
            std::cout << "Memory used     : " << mem_used << "MB\n";
            std::cout << "Time used       : " << time_used << "s\n\n";
            time_samples.push_back(time_used);
            mem_samples.push_back(mem_used);
            start = std::chrono::high_resolution_clock::now();
        }
        if (i == test_size) {
            break;
        }
        std::getline(testfile, key);
        zdd.Set(key, 1);
    }

    PrintResults(test_size, type, step, time_samples, mem_samples, tests_dir,
                 test_name);
}
}  // namespace TEST

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cerr << "wrong number of args\n";
        return 1;
    }

    uint32_t key_byte_len = std::stoul(argv[1]);
    uint32_t test_size = std::stoul(argv[2]);
    std::string compression_type = argv[3];
    std::string tests_dir = argv[4];
    std::string test_name = argv[5];

    auto compression = [&compression_type]() {
        if (compression_type == "zstd") {
            return Compression::compression::zstd;
        } else if (compression_type == "md5") {
            return Compression::compression::md5;
        } else if (compression_type == "sha256") {
            return Compression::compression::sha256;
        } else {
            return Compression::compression::none;
        }
    };

    TEST::test(key_byte_len, test_size, compression(), tests_dir, test_name);

    return 0;
}
