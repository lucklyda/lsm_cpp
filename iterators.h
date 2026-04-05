#pragma once
#include <cstdint>
#include <string_view>
#include "key.h"

class Iterators {
public:
    Iterators() = default;
    virtual ~Iterators() = default;
    virtual LsmKeyView key_view() const = 0;
    virtual std::string_view value_view() const = 0;
    virtual bool is_valid() = 0;
    virtual bool next() = 0;
    virtual uint32_t num_active_iterators() { return 1; }
};
