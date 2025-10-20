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
};

class ZstdCompressor : public ICompressor {
public:
    virtual std::string Compress(const std::string& key) const;
};

class MD5Hasher : public ICompressor {
public:
    virtual std::string Compress(const std::string& key) const;
};

class SHA256Hasher : public ICompressor {
public:
    virtual std::string Compress(const std::string& key) const;
};

class NoCompression : public ICompressor {
public:
    virtual std::string Compress(const std::string& key) const;
};

std::unique_ptr<ICompressor> BuildCompressor(compression type);
}  // namespace Compression
