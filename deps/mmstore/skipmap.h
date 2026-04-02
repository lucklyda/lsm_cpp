#pragma once

#include "mmstore.h"
#include <vector>
#include <random>
#include <stdexcept>
#include <mutex>
template<typename Key, typename Value>
class SkipmapIterator;

template<typename Key, typename Value>
struct Node {
    Key key;
    Value value;
    std::vector<Node*> next;      // 每层的后继指针
    Node(const Key& k, const Value& v, int level)
        : key(k), value(v), next(level, nullptr) {}
};

template<typename Key, typename Value>
class SkipMap:public mmstore<Key,Value>
{
    friend class SkipmapIterator<Key,Value>;
private:
    Node<Key,Value>* head_;                     // 哨兵头节点
    int max_level_;                  // 最大层数
    int cur_level_;                  // 当前实际最高层数
    std::default_random_engine rng_;
    std::uniform_real_distribution<double> dist_;
    mutable std::mutex mtx_;

    int random_level(){
        int level = 1;
        while (dist_(rng_) < 0.5 && level < max_level_)
            ++level;
        return level;
    }
    std::vector<Node<Key,Value>*> find_predecessors(const Key& key)
    {
        std::vector<Node<Key,Value>*> preds(cur_level_ + 1, nullptr);
        Node<Key,Value>* cur = head_;
        for (int i = cur_level_; i >= 0; --i) {
            while (cur->next[i] && cur->next[i]->key < key)
                cur = cur->next[i];
            preds[i] = cur;
        }
        return preds;
    }
    Node<Key,Value>* find_node(const Key& key)
    {
        Node<Key,Value>* cur = head_;
        for (int i = cur_level_; i >= 0; --i) {
            while (cur->next[i] && cur->next[i]->key < key)
                cur = cur->next[i];
        }
        cur = cur->next[0];
        if (cur && cur->key == key)
            return cur;
        return nullptr;
    }

public:
    explicit SkipMap(int max_level = 32):
        max_level_(max_level), cur_level_(0),
        rng_(std::random_device{}()),
        dist_(0.0, 1.0)
    {
        head_ = new Node<Key,Value>(Key{}, Value{}, max_level_);
    }

    ~SkipMap(){
        Node<Key,Value>* cur = head_->next[0];
        while (cur) {
            Node<Key,Value>* next = cur->next[0];
            delete cur;
            cur = next;
        }
        delete head_;
    }

    bool is_empty() override{
        return head_->next[0]==nullptr;
    }

    void put(const Key& key, const Value& value) override
    {
        std::lock_guard<std::mutex> lock(mtx_);
        Node<Key,Value>* existing = find_node(key);
        if (existing) {
            existing->value = value;
            return;
        }

        int new_level = random_level();
        if (new_level > cur_level_)
            cur_level_ = new_level;

        Node<Key,Value>* new_node = new Node<Key,Value>(key, value, new_level);
        std::vector<Node<Key,Value>*> preds = find_predecessors(key); // 此时 preds 大小已 >= new_level

        for (int i = 0; i < new_level; ++i) {
            new_node->next[i] = preds[i]->next[i];
            preds[i]->next[i] = new_node;
        }
    }

    Value& get(const Key& key) override
    {
        std::lock_guard<std::mutex> lock(mtx_);
        Node<Key,Value>* node = find_node(key);
        if (!node)
            throw std::out_of_range("Key not found");
        return node->value;
    }


    mmstore_iterator<Key,Value>* create_iterator(const Bound<Key>& left,const Bound<Key>& right) override
    {
        return new SkipmapIterator<Key,Value>(this,left,right);
    }
    mmstore_iterator<Key,Value>* create_iterator() override
    {
        Bound<Key> left;
        Bound<Key> right;
        left.type=0;
        right.type=0;
        return new SkipmapIterator<Key,Value>(this,left,right);
    }
};

template<typename Key, typename Value>
class SkipmapIterator : public mmstore_iterator<Key, Value> {
private:
    const SkipMap<Key, Value>* map_;
    Node<Key, Value>* current_;

    bool satisfies_lower(const Key& key) const {
        if (this->lower_.type == 0) return true;
        if (this->lower_.type == 1)
            return !(key < this->lower_.key);
        else
            return this->lower_.key < key;
    }

    bool satisfies_upper(const Key& key) const {
        if (this->upper_.type == 0) return true;
        if (this->upper_.type == 1)
            return !(this->upper_.key < key);
        else
            return key < this->upper_.key;
    }

    Node<Key, Value>* find_start() const {
        if (!map_) return nullptr;

        // 无下界 → 从头开始
        if (this->lower_.type == 0) {
            return map_->head_->next[0];
        }

        Node<Key, Value>* cur = map_->head_;
        // 安全遍历层级，避免无符号递减死循环
        for (int i = static_cast<int>(map_->cur_level_); i >= 0; --i) {
            while (cur->next[i] && cur->next[i]->key < this->lower_.key) {
                cur = cur->next[i];
            }
        }

        cur = cur->next[0];
        if (!cur) return nullptr;

        // 左开区间：等于下界要跳过
        if (this->lower_.type == 2 && cur->key == this->lower_.key) {
            cur = cur->next[0];
        }

        // 最终检查是否满足下界
        if (cur && !satisfies_lower(cur->key)) {
            return nullptr;
        }

        return cur;
    }

public:
    SkipmapIterator(const SkipMap<Key, Value>* skip_map,
                    const Bound<Key>& lower,
                    const Bound<Key>& upper)
        : mmstore_iterator<Key, Value>(lower, upper),
          map_(skip_map)
    {
        current_ = find_start();

        // 检查 upper
        if (current_ && !satisfies_upper(current_->key)) {
            current_ = nullptr;
        }
    }

    bool is_valid() const noexcept override {
        if (!current_ || !map_) return false;
        return satisfies_lower(current_->key) && satisfies_upper(current_->key);
    }

    void next() override {
        if (!current_) return;

        current_ = current_->next[0];

        // 跳过不满足边界的节点
        while (current_) {
            if (satisfies_lower(current_->key) && satisfies_upper(current_->key)) {
                break;
            }
            if (!satisfies_upper(current_->key)) {
                current_ = nullptr;
                break;
            }
            current_ = current_->next[0];
        }
    }

    const Key& get_key() const override {
        if (!is_valid()) throw std::logic_error("Iterator invalid");
        return current_->key;
    }

    const Value& get_value() const override {
        if (!is_valid()) throw std::logic_error("Iterator invalid");
        return current_->value;
    }
};

