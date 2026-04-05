#pragma once

#include <string_view>
#include <iostream>
#include <string.h>
#include <nmmintrin.h>
#include <vector>
using usize = uint64_t;
#define TS_BEGIN UINT64_MAX
#define TS_END 0

//TOOD 替换所有string,使用内存池管理数据,保证安全的生命周期
struct LsmKey {
    std::string user_key;
    uint64_t ts;

    bool operator<(const LsmKey& other) const {
        if (user_key != other.user_key) {
            return user_key < other.user_key;
        }
        return ts > other.ts;
    }

    int cmp(const LsmKey& other) const {
        int cmp = user_key.compare(other.user_key);
        if (cmp != 0) return cmp < 0 ? -1 : 1;
        if (ts > other.ts) return -1;   // 时间戳大的排在前面
        if (ts < other.ts) return 1;
        return 0;
    }

    bool operator>(const LsmKey& other) const {
        return other < *this;
    }

    // LsmKey& operator=(const LsmKey& other){
    //     user_key = other.user_key;
    //     ts = other.ts;
    //     return *this;
    // }
    bool operator==(const LsmKey& other) const {
        return user_key == other.user_key && ts == other.ts;
    }
    LsmKey(const LsmKey&) = default;
    LsmKey() {
        user_key = "";
        ts = 0;
    }

    LsmKey(std::string k,uint64_t ts_):user_key(std::move(k)),ts(ts_){}

    LsmKey(std::string_view k,uint64_t ts_):user_key(k),ts(ts_){}
};


struct LsmValue
{
    std::string value;

    bool is_empty()const{
        return value.size()==0;
    }
    uint64_t size()const{
        return value.size();
    }

    const char* to_data()const{
        return value.c_str();
    }

    LsmValue(const std::string &s):value(s){}
    LsmValue(){}
    LsmValue(const char*s):value(s){}
};


class StringRef {
    public:
        StringRef() : data_(nullptr), len_(0) {}
        StringRef(const char* data, size_t len) : data_(data), len_(len) {}
        StringRef(const char* str) : data_(str), len_(str ? std::strlen(str) : 0) {}
        StringRef(const std::string& s) : data_(s.data()), len_(s.size()) {}
        StringRef(std::string_view sv) : data_(sv.data()), len_(sv.size()) {}
    
        const char* data() const { return data_; }
        size_t size() const { return len_; }
        size_t length() const { return len_;}
        bool empty() const { return len_ == 0; }
        void clear() { len_=0; data_ = nullptr;}
    
        const char& operator[](size_t idx) const {
            // 假设 idx 一定在 [0, len_) 范围内，调用者需保证
            return data_[idx];
        }
    
        // 比较函数，直接内存比较
        int compare(const StringRef& other) const {
            int cmp = std::memcmp(data_, other.data_, std::min(len_, other.len_));
            if (cmp != 0) return cmp;
            if (len_ < other.len_) return -1;
            if (len_ > other.len_) return 1;
            return 0;
        }
    
        int compare(std::string_view sv) const {
            int cmp = std::memcmp(data_, sv.data(), std::min(len_, sv.size()));
            if (cmp != 0) return cmp;
            if (len_ < sv.size()) return -1;
            if (len_ > sv.size()) return 1;
            return 0;
        }
    
        // 与 const char* 比较（可选）
        int compare(const char* str) const {
            return compare(std::string_view(str));
        }
    
        bool operator==(const StringRef& other) const { return compare(other) == 0; }
        bool operator!=(const StringRef& other) const { return !(*this == other); }
        bool operator<(const StringRef& other) const { return compare(other) < 0; }
        bool operator<=(const StringRef& other) const { return compare(other) <= 0; }
        bool operator>(const StringRef& other) const { return compare(other) > 0; }
        bool operator>=(const StringRef& other) const { return compare(other) >= 0; }
    
        // 与 std::string_view 的比较（成员函数）
        bool operator==(std::string_view sv) const { return compare(sv) == 0; }
        bool operator!=(std::string_view sv) const { return !(*this == sv); }
        bool operator<(std::string_view sv) const { return compare(sv) < 0; }
        bool operator<=(std::string_view sv) const { return compare(sv) <= 0; }
        bool operator>(std::string_view sv) const { return compare(sv) > 0; }
        bool operator>=(std::string_view sv) const { return compare(sv) >= 0; }
    
        // 转换为 std::string_view（零拷贝）
        operator std::string_view() const { return {data_, len_}; }
    
    private:
        const char* data_;
        size_t len_;
    };
    
    inline bool operator==(std::string_view lhs, const StringRef& rhs) {
        return rhs == lhs;   // 复用成员
    }
    inline bool operator!=(std::string_view lhs, const StringRef& rhs) {
        return !(lhs == rhs);
    }
    inline bool operator<(std::string_view lhs, const StringRef& rhs) {
        return rhs > lhs;    // 或者直接用 rhs.compare(lhs) > 0
    }
    inline bool operator<=(std::string_view lhs, const StringRef& rhs) {
        return rhs >= lhs;
    }
    inline bool operator>(std::string_view lhs, const StringRef& rhs) {
        return rhs < lhs;
    }
    inline bool operator>=(std::string_view lhs, const StringRef& rhs) {
        return rhs <= lhs;
    }

// 迭代器 / 比较用：零拷贝视图（底层缓冲区生命周期由具体迭代器保证）
struct LsmKeyView {
    std::string_view user_key;
    uint64_t ts = 0;
};

inline int cmp_lsm_key_view(LsmKeyView a, LsmKeyView b) noexcept {
    int c = a.user_key.compare(b.user_key);
    if (c != 0) {
        return c < 0 ? -1 : 1;
    }
    if (a.ts > b.ts) {
        return -1;
    }
    if (a.ts < b.ts) {
        return 1;
    }
    return 0;
}

inline bool lsm_key_view_eq(LsmKeyView a, LsmKeyView b) noexcept {
    return cmp_lsm_key_view(a, b) == 0;
}

inline bool lsm_key_view_lt(LsmKeyView a, LsmKeyView b) noexcept {
    return cmp_lsm_key_view(a, b) < 0;
}

struct MemLsmKey {
    StringRef user_key;
    uint64_t ts;

    bool operator<(const MemLsmKey& other) const {
        int cmp = user_key.compare(other.user_key);
        if (cmp != 0) return cmp < 0;
        return ts > other.ts;
    }

    bool operator<=(const MemLsmKey& other) const {
        return !(*this > other);
    }

    int cmp(const MemLsmKey& other) const {
        int c = user_key.compare(other.user_key);
        if (c != 0) return c;
        if (ts > other.ts) return -1;
        if (ts < other.ts) return 1;
        return 0;
    }

    bool operator>(const MemLsmKey& other) const { return other < *this; }
    bool operator==(const MemLsmKey& other) const {
        return user_key == other.user_key && ts == other.ts;
    }

    MemLsmKey() : user_key(), ts(0) {}
    MemLsmKey(StringRef key, uint64_t ts_) : user_key(key), ts(ts_) {}
    MemLsmKey(const MemLsmKey&) = default;
    MemLsmKey& operator=(const MemLsmKey&) = default;

    LsmKeyView view() const {
        return {std::string_view(user_key.data(), user_key.size()), ts};
    }
};

struct MemLsmValue {
    StringRef value;

    bool is_empty() const { return value.empty(); }
    size_t size() const { return value.size(); }
    const char* to_data() const { return value.data(); }

    MemLsmValue() : value() {}
    explicit MemLsmValue(StringRef v) : value(v) {}
    MemLsmValue(const char* data) : value(data) {}
    MemLsmValue(const std::string& s) : value(s) {}
    MemLsmValue(const MemLsmValue&) = default;
    MemLsmValue& operator=(const MemLsmValue&) = default;

    std::string_view view() const {
        return std::string_view(value.data(), value.size());
    }
};

// 方便打印
// static  std::ostream& operator<<(std::ostream& os, const LsmKey& k) {
//     os << "[key=" << k.user_key << ", ts=" << k.ts << "]";
//     return os;
// }

using Key = LsmKey;
using Value = LsmValue;

class CharBuffer {
public:
    CharBuffer(size_t init_cap = 64) {
        capacity_ = init_cap;
        data_ = new char[capacity_];
        size_ = 0;
    }

    ~CharBuffer() {
        delete[] data_;
    }

    // 禁止拷贝（避免浅拷贝 double free）
    CharBuffer(const CharBuffer&) = delete;
    CharBuffer& operator=(const CharBuffer&) = delete;

    // 支持移动（高效转移所有权）
    CharBuffer(CharBuffer&& other) noexcept {
        data_ = other.data_;
        size_ = other.size_;
        capacity_ = other.capacity_;
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }

    CharBuffer& operator=(CharBuffer&& other) noexcept {
        if (this != &other) {
            delete[] data_;
            data_ = other.data_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            other.data_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
        }
        return *this;
    }
    void append(const char* data, size_t len) {
        if (!data || len == 0) return;
        if (size_ + len > capacity_) {
            reserve(size_ + len);
        }
        memcpy(data_ + size_, data, len);
        size_ += len;
    }

    void reserve(size_t min_cap) {
        if (min_cap <= capacity_) return;
        size_t new_cap = capacity_ * 2;
        if (new_cap < min_cap) new_cap = min_cap;

        char* new_data = new char[new_cap];
        if (size_ > 0) {
            memcpy(new_data, data_, size_);
        }

        delete[] data_;
        data_ = new_data;
        capacity_ = new_cap;
    }

    void clear() {
        size_ = 0;
    }

    char* data()             { return data_; }
    const char* data() const { return data_; }
    size_t size() const      { return size_; }
    size_t capacity() const  { return capacity_; }

private:
    char* data_ = nullptr;   // 真实缓冲区
    size_t size_ = 0;       // 已使用长度
    size_t capacity_ = 0;   // 总容量
};

static  uint32_t crc32c_hw(const char* data, size_t len) {
    uint32_t crc = 0;
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; ++i) {
        crc = _mm_crc32_u8(crc, p[i]);
    }
    return crc;
}


static uint32_t Fetch32(const char *p) {
  uint32_t result;
  memcpy(&result, p, sizeof(result));
  return result;
}

static  uint32_t Fingerprint32(const char *s, size_t len) {
  if (len <= 4) {
    uint32_t a = 0, b = 0, c = 0;
    switch (len) {
      case 3: a += s[2] << 16;
      case 2: a += s[1] << 8;
      case 1: a += s[0];
              a *= 0x9E3779B1;
              break;
      case 4: b = Fetch32(s);
    }
    c = 0x9E3779B1 * (len + a);
    c ^= b;
    c ^= c >> 17;
    c *= 0x85EBCA77;
    c ^= c >> 13;
    c *= 0xC2B2AE3D;
    c ^= c >> 16;
    return c;
  }

  uint32_t x = Fetch32(s), y = Fetch32(s + len - 4);
  uint32_t z = len * 0x9E3779B1 + x + y;
  const char *p = s + 4;
  for (size_t i = 0; i < (len - 4) >> 3; i++) {
    x = Fetch32(p);
    y = Fetch32(p + 4);
    z += y;
    z ^= x;
    p += 8;
  }
  z ^= z >> 17;
  z *= 0x85EBCA77;
  z ^= z >> 13;
  z *= 0xC2B2AE3D;
  z ^= z >> 16;
  return z;
}


struct LevelItem
{
    uint64_t level_num;
    std::vector<uint64_t> sst_ids;
};



