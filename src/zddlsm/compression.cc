#include "include/compression.h"

#include <openssl/md5.h>
#include <openssl/sha.h>
#include <zstd.h>

#include <stdexcept>
#include <vector>

namespace Compression {
constexpr static uint32_t COMPRESSION_LEVEL_ZSTD = 20;

std::string ZstdCompressor::Compress(const std::string& key) const {
    if (key.size() == 0) {
        return "";
    }

    uint32_t compressed_size_ub = ZSTD_compressBound(key.size());
    std::vector<char> buffer(compressed_size_ub);
    uint32_t compressed_size =
        ZSTD_compress(buffer.data(), compressed_size_ub, key.data(), key.size(),
                      COMPRESSION_LEVEL_ZSTD);

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error(ZSTD_getErrorName(compressed_size));
    }

    return std::string(buffer.data(), compressed_size);
}

std::string MD5Hasher::Compress(const std::string& key) const {
    std::string compressed_key(MD5_DIGEST_LENGTH, '\0');
    MD5(reinterpret_cast<const unsigned char*>(key.data()), key.size(),
        reinterpret_cast<unsigned char*>(compressed_key.data()));

    return compressed_key;
}

std::string SHA256Hasher::Compress(const std::string& key) const {
    std::string compressed_key(SHA256_DIGEST_LENGTH, '\0');
    SHA256(reinterpret_cast<const unsigned char*>(key.data()), key.size(),
           reinterpret_cast<unsigned char*>(compressed_key.data()));

    return compressed_key;
}

std::string NoCompression::Compress(const std::string& key) const {
    return key;
}

std::unique_ptr<ICompressor> BuildCompressor(compression type) {
    switch (type) {
        case compression::md5:
            return std::make_unique<MD5Hasher>();
        case compression::sha256:
            return std::make_unique<SHA256Hasher>();
        case compression::zstd:
            return std::make_unique<ZstdCompressor>();
        default:
            return std::make_unique<NoCompression>();
    }
}
}  // namespace Compression