#pragma once
#include <vector>
#include<stdint.h>
#include <cstddef>
#include <cmath>
#include <algorithm>

class BitSlice {
public:
    virtual bool get_bit(size_t idx) const = 0;
    virtual size_t bit_len() const = 0;
};

class BitSliceMut {
public:
    virtual void set_bit(size_t idx, bool val) = 0;
};

// 给 std::vector<uint8_t> 实现位操作
template<typename T>
struct VectorBitSlice : BitSlice {
    const T& data;
    explicit VectorBitSlice(const T& d) : data(d) {}

    bool get_bit(size_t idx) const override {
        size_t pos = idx / 8;
        size_t offset = idx % 8;
        return (data[pos] & (1 << offset)) != 0;
    }

    size_t bit_len() const override {
        return data.size() * 8;
    }
};

template<typename T>
struct VectorBitSliceMut : VectorBitSlice<T>, BitSliceMut {
    T& data;
    explicit VectorBitSliceMut(T& d) : VectorBitSlice<T>(d), data(d) {}

    void set_bit(size_t idx, bool val) override {
        size_t pos = idx / 8;
        size_t offset = idx % 8;
        if (val) {
            data[pos] |= (1 << offset);
        } else {
            data[pos] &= ~(1 << offset);
        }
    }
};

struct Bloom {
    /// data of filter in bits
    std::vector<uint8_t> filter;
    /// number of hash functions
    uint8_t k;

    /// Decode a bloom filter
    static Bloom decode(const std::vector<uint8_t>& buf) {
        if (buf.empty()) {
            return Bloom{std::vector<uint8_t>(),0};
        }
        Bloom res;
        res.filter = std::vector<uint8_t>(buf.begin(), buf.end() - 1);
        res.k = buf.back();
        return res;
    }

    static Bloom decode(const char* buf, size_t len) {
        std::vector<uint8_t> vec(reinterpret_cast<const uint8_t*>(buf), 
                                reinterpret_cast<const uint8_t*>(buf) + len);
        return decode(vec);
    }

    /// Encode a bloom filter
    void encode(std::vector<uint8_t>& buf) const {
        buf.insert(buf.end(), filter.begin(), filter.end());
        buf.push_back(k);
    }

    /// P = (1-e^(-kn/m))^k ~= e^(-nk²/m)
    /// P: false positive rate, n: entries, m: bits, k: hash count
    /// Optimal size = -n * ln(P) / (ln(2))²
    static size_t bloom_bits_per_key(size_t entries, double false_positive_rate) {
        double size = - (double)entries * log(false_positive_rate) / pow(log(2.0), 2);
        double locs = ceil(size / (double)entries);
        return (size_t)locs;
    }

    /// Build bloom filter from key hashes
    static Bloom build_from_key_hashes(const std::vector<uint32_t>& keys, size_t bits_per_key) {
        uint32_t k = (uint32_t)((double)bits_per_key * 0.69);
        k = std::clamp(k, 1u, 30u);
        size_t nbits = std::max(keys.size() * bits_per_key, (size_t)64);
        size_t nbytes = (nbits + 7) / 8;
        nbits = nbytes * 8;

        std::vector<uint8_t> filter(nbytes, 0);
        VectorBitSliceMut mut_bits(filter);

        for (uint32_t h : keys) {
            uint32_t h_val = h;
            uint32_t delta = h_val << 15 | h_val >> 17; // rotate_left(15)
            for (uint32_t i = 0; i < k; ++i) {
                size_t bit_pos = (size_t)h_val % nbits;
                mut_bits.set_bit(bit_pos, true);
                h_val = h_val + delta; // wrapping add
            }
        }

        Bloom res;
        res.filter = std::move(filter);
        res.k = (uint8_t)k;
        return res;
    }

    /// Check if a bloom filter may contain some data
    bool may_contain(uint32_t h) const {
        if (k > 30) {
            return true;
        }
        VectorBitSlice bits(filter);
        size_t nbits = bits.bit_len();
        uint32_t delta = h << 15 | h >> 17;
        uint32_t h_val = h;

        for (uint8_t i = 0; i < k; ++i) {
            size_t bit_pos = (size_t)h_val % nbits;
            if (!bits.get_bit(bit_pos)) {
                return false;
            }
            h_val = h_val + delta;
        }
        return true;
    }
};

