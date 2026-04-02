#include <vector>
#include <cstring>
#include <utility>
#include <stdexcept>

/**
 * @brief 缓冲写入器，包装底层 Writer 并提供缓冲写入功能。
 *
 * @tparam Writer 底层写入器类型，必须支持 write(const char*, size_t) 方法，
 *                返回实际写入的字节数（size_t），并在失败时抛出异常。
 */
template <typename Writer>
class BufWriter {
public:
    /**
     * @brief 构造一个 BufWriter。
     * @param writer 底层写入器对象（将被移动）。
     * @param capacity 内部缓冲区大小，默认为 8KB。
     */
    explicit BufWriter(Writer writer, size_t capacity = 8192)
        : writer_(std::move(writer)), buffer_(capacity), pos_(0) {}

    // 禁止拷贝
    BufWriter(const BufWriter&) = delete;
    BufWriter& operator=(const BufWriter&) = delete;

    // 移动构造（noexcept 取决于 Writer 的移动）
    BufWriter(BufWriter&& other) noexcept
        : writer_(std::move(other.writer_)),
          buffer_(std::move(other.buffer_)),
          pos_(other.pos_) {
        other.pos_ = 0;  // 防止原对象析构时刷新已移动的数据
    }

    // 移动赋值
    BufWriter& operator=(BufWriter&& other) noexcept {
        if (this != &other) {
            flush();                      // 先刷新当前对象
            writer_ = std::move(other.writer_);
            buffer_ = std::move(other.buffer_);
            pos_ = other.pos_;
            other.pos_ = 0;
        }
        return *this;
    }

    ~BufWriter() {
        try {
            flush();
        } catch (...) {
            // 析构函数不能抛出异常，忽略刷新错误
        }
    }

    /**
     * @brief 将数据写入缓冲区。
     * @param data 数据指针。
     * @param len  数据长度。
     */
    void write(const char* data, size_t len) {
        if (len == 0) return;

        size_t remaining = buffer_.size() - pos_;
        if (len > remaining) {
            // 缓冲区空间不足，先刷新
            flush();

            // 如果数据大于整个缓冲区，直接写入底层（避免复制）
            if (len > buffer_.size()) {
                write_all(data, len);
                return;
            }
            // 否则，数据可以放入空缓冲区，继续使用缓冲
            remaining = buffer_.size();
        }

        // 复制到缓冲区
        std::memcpy(buffer_.data() + pos_, data, len);
        pos_ += len;

        // 若缓冲区满，立即刷新
        if (pos_ == buffer_.size()) {
            flush();
        }
    }

    /**
     * @brief 写入单个字符。
     * @param c 要写入的字符。
     */
    void write(char c) {
        if (pos_ >= buffer_.size()) {
            flush();
        }
        buffer_[pos_++] = c;
        if (pos_ == buffer_.size()) {
            flush();
        }
    }

    /**
     * @brief 强制将缓冲区内容写入底层。
     * @throws 可能抛出底层 Writer 的异常。
     */
    void flush() {
        if (pos_ > 0) {
            write_all(buffer_.data(), pos_);
            pos_ = 0;
        }
    }

    /// 返回底层写入器的可变引用。
    Writer& get_mut() { return writer_; }
    /// 返回底层写入器的常量引用。
    const Writer& get_ref() const { return writer_; }

    /// 返回内部缓冲区的常量引用（只读）。
    const std::vector<char>& buffer() const { return buffer_; }

    /// 返回当前缓冲区中已缓存但未写入的字节数。
    size_t buffered_bytes() const { return pos_; }

    /// 返回缓冲区的总容量（字节）。
    size_t capacity() const { return buffer_.size(); }

    /**
     * @brief 取出底层写入器（先刷新缓冲区）。
     * @return 底层写入器对象。
     * @throws 可能抛出 flush() 中的异常。
     */
    Writer into_inner() {
        flush();
        return std::move(writer_);
    }

private:
    /**
     * @brief 确保全部数据被写入底层（处理部分写入）。
     * @param data 数据指针。
     * @param len  数据长度。
     * @throws 如果底层 write 返回 0 或抛出异常。
     */
    void write_all(const char* data, size_t len) {
        size_t written = 0;
        while (written < len) {
            size_t n = writer_.write(data + written, len - written);
            if (n == 0) {
                throw std::runtime_error("write_all failed: write returned 0");
            }
            written += n;
        }
    }

    Writer writer_;              // 底层写入器
    std::vector<char> buffer_;   // 缓冲区
    size_t pos_;                 // 缓冲区中已填充的字节数
};