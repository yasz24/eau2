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

    size_t hash() {
        size_t hash_ = 0;
        hash_ += this->name_->hash();
        hash_ = hash_ * this->node_; 
    }

    bool equals(Object* other) {
        if (other == this) return true;
        Key* x = dynamic_cast<Key *>(other);
        if (x == nullptr) return false;
        return this->name_->equals(x->name_) && (this->node_ == x->node_);
    }
};