#pragma once
#include <map>
#include <optional>
#include <utility>
#include <stdint.h>

class Watermark {
public:
    Watermark() = default;

    void add_reader(uint64_t ts) {
        readers_[ts]++;
    }

    void remove_reader(uint64_t ts) {
        auto it = readers_.find(ts);
        if (it == readers_.end()) return;

        it->second--;
        if (it->second == 0) {
            readers_.erase(it);
        }
    }

    size_t num_retained_snapshots() const {
        return readers_.size();
    }

    std::optional<uint64_t> watermark() const {
        if (readers_.empty()) return std::nullopt;
        return readers_.begin()->first;
    }

private:
    std::map<uint64_t, size_t> readers_;
};