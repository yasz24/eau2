#pragma once
#include "../object.h"
//todo: write hash, equals
class Value : public Object{
public:
    char* data; //owned
    size_t length; 

    Value(char* data, size_t length) {
        this->data = data;
        this->length = length;
    }

    size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < this->length; ++i)
            hash = data[i] + (hash << 6) + (hash << 16) - hash;
        return hash;
    }

    bool equals(Object* other) {
        if (other == this) return true;
        Value* x = dynamic_cast<Value *>(other);
        if (x == nullptr) return false;
        if (this->length != x->length) return false;
        return strncmp(data, x->data, length) == 0;
    }
};