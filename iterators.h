#pragma once
#include<cstdint>
#include "key.h"
class Iterators
{
private:
    /* data */
public:
    Iterators(/* args */) = default;
    virtual ~Iterators() = default;
    virtual const Key& key()const=0;
    virtual Value value()const=0;
    virtual bool is_valid()=0;
    virtual bool next()=0;
    virtual uint32_t num_active_iterators(){
        return 1;
    }
};

