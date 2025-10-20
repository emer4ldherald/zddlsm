#include <memory>
#include <string>

namespace Compression {
enum class compression {
    none,
    md5,
    sha256,
    zstd,
};

class ICompressor {
public:
    virtual ~ICompressor() = default;

    virtual std::string Compress(const std::string& key) const = 0;

    virtual uint32_t BytesNeeds(uint32_t key_byte_len) const = 0;
};

class ZstdCompressor : public ICompressor {
public:
    std::string Compress(const std::string& key) const;

    uint32_t BytesNeeds(uint32_t key_byte_len) const;
};

class MD5Hasher : public ICompressor {
public:
    std::string Compress(const std::string& key) const;

    uint32_t BytesNeeds(uint32_t key_byte_len) const;
};

class SHA256Hasher : public ICompressor {
public:
    std::string Compress(const std::string& key) const;

    uint32_t BytesNeeds(uint32_t key_byte_len) const;
};

class NoCompression : public ICompressor {
public:
    std::string Compress(const std::string& key) const;

    uint32_t BytesNeeds(uint32_t key_byte_len) const;
};

std::unique_ptr<ICompressor> BuildCompressor(compression type);
}  // namespace Compression
