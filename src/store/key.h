#pragma once
#include "../object.h"
#include "../utils/string.h" 

//todo: write hash, equals.
class Key : public Object{
public:
    String* name_; //owned
    size_t node_; 

    Key(char* name, size_t node) {
        this->name_ = new String(name);
        this->node_ = node;
    }

    size_t node() {
        return this->node_;
    }
};