#pragma once

#include "mmstore.h"
#include <map>

template<typename Key, typename Value>
class MapIterator;

template<typename Key, typename Value>
class Map : public mmstore<Key,Value>
{

public:
    /* data */
    std::map<Key,Value> map;
public:
    Map() = default;
    ~Map() = default;

    bool is_empty() override{
        return map.size()==0;
    }

    void put(const Key& key, const Value& value) override
    {
        auto it = map.find(key);
        if(it == map.end()){
            map.emplace(key,value);
        }else{
            it->second = value;
        }
        
    }

    Value& get(const Key& key) override{
        return map.at(key);
    }


    mmstore_iterator<Key,Value>* create_iterator(const Bound<Key>& left,const Bound<Key>& right) override
    {
        return new MapIterator<Key,Value>(this,left,right);
    }
    mmstore_iterator<Key,Value>* create_iterator() override
    {
        Bound<Key> left;
        Bound<Key> right;
        left.type=0;
        right.type=0;
        return new MapIterator<Key,Value>(this,left,right);
    }

};


template<typename Key, typename Value>
class MapIterator : public mmstore_iterator<Key, Value> {
private:
    const Map<Key,Value> *map_;
    typename std::map<Key, Value>::const_iterator cit;

private:
    bool check_lower()const{
        if(this->lower_.type==0)return true;
        const Key& key = cit->first;
        const Key& bound_val = this->lower_.key;
        if(this->lower_.type == 1)return !(key<bound_val);// key>=bound
        if(this->lower_.type==2)return bound_val<key;//key > bound
        return true;
    }
    bool check_upper()const{
        if(this->upper_.type==0)return true;
        const Key& key = cit->first;
        const Key& bound_val = this->upper_.key;
        if(this->upper_.type == 1)return !(bound_val<key);// key<=bound
        if(this->upper_.type==2)return key < bound_val;//key<bound
        return true;
    }

public:
    MapIterator(const Map<Key,Value> *map,const Bound<Key>& lower,
                    const Bound<Key>& upper):
                     mmstore_iterator<Key, Value>(lower, upper),map_(map)
        {
            if(this->lower_.type == 0)cit = map_->map.cbegin();
            else cit = map_->map.lower_bound(this->lower_.key);

            while (cit != map_->map.end() && !check_lower()) {
                ++cit;
            }
        }
        bool is_valid() const noexcept override {
            if (cit == map_->map.end()) return false;
            return check_lower() && check_upper();
        }
        void next() override {
            if (cit == map_->map.end()) return;
            ++cit;
        }

        const Key& get_key() const override {
            return cit->first;
        }

        const Value& get_value() const override{
            return cit->second;
        }

        ~MapIterator() = default;
};

