#pragma once
#include <type_traits>
#include <cstdint>

template<typename, typename = void>
struct has_less_operator : std::false_type {};

template<typename T>
struct has_less_operator<T, std::void_t<decltype(std::declval<T>() < std::declval<T>())>>
    : std::true_type {};

template<typename T>
inline constexpr bool has_less_operator_v = has_less_operator<T>::value;

template<typename, typename = void>
struct has_equal_operator : std::false_type {};

template<typename T>
struct has_equal_operator<T, std::void_t<decltype(std::declval<T>() == std::declval<T>())>>
    : std::true_type {};

template<typename T>
inline constexpr bool has_equal_operator_v = has_equal_operator<T>::value;

template<typename Key, typename Value>
class mmstore;

template<typename Key>
struct Bound
{
    uint8_t type;//0 is none; [ is 1;( is2
    Key key;

    static Bound none() { return {}; }
    static Bound closed(const Key& k) { return {1, k}; }
    static Bound open(const Key& k) { return {2, k}; }
};


template<typename Key, typename Value>
class mmstore_iterator {
    friend class mmstore<Key, Value>;
protected:
    explicit mmstore_iterator(const Bound<Key>& left,const Bound<Key>& right)
    :lower_(left),upper_(right)
    {

    }
public:
    virtual ~mmstore_iterator() = default;

    virtual bool is_valid() const noexcept = 0;
    virtual void next() = 0;
    virtual const Key& get_key() const = 0;
    virtual const Value& get_value() const = 0;

    mmstore_iterator(const mmstore_iterator&) = delete;
    mmstore_iterator& operator=(const mmstore_iterator&) = delete;
    mmstore_iterator(mmstore_iterator&&) = delete;
    mmstore_iterator& operator=(mmstore_iterator&&) = delete;
protected:
    Bound<Key> lower_;
    Bound<Key> upper_;
};

template<typename Key,typename Value>
class mmstore
{
    static_assert(std::is_assignable_v<Key&, const Key&>,
                  "Key must be assignable (support operator=)");
    static_assert(has_less_operator_v<Key>,
                  "Key must support operator<");
    static_assert(has_equal_operator_v<Key>,
                  "Key must support operator==");
                  
public:
    mmstore(/* args */) = default;
    virtual ~mmstore() = default;

    virtual void put(const Key &key,const Value& value)=0;
    virtual Value& get(const Key &key) = 0;
    virtual mmstore_iterator<Key,Value>* create_iterator(const Bound<Key>& left,const Bound<Key>& right)=0;
    virtual mmstore_iterator<Key,Value>* create_iterator()=0;
    virtual bool is_empty()=0;
};




