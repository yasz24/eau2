#pragma once
#include "../object.h"
#include "../utils/string.h" 
#include "../serialize/serial.h"
#include "../serialize/jsonHelper.h"

//todo: write hash, equals.
class Key : public Object{
public:
    String* name_; //owned
    size_t node_; 

    Key(char* name, size_t node) {
        this->name_ = new String(name);
        this->node_ = node;
    }

    Key(char* serialized) {
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->name_ = new String(JSONHelper::getValueFromKey("name_", payload)->c_str());
        this->node_ = std::stoi(JSONHelper::getValueFromKey("node_", payload)->c_str());
    }

    size_t node() {
        return this->node_;
    }

    size_t hash() {
        size_t _hash = 0;
        _hash += this->name_->hash();
        //std::cout << "Key hash: " << _hash << "\n";
    }

    bool equals(Object* other) {
        if (other == this) return true;
        Key* x = dynamic_cast<Key *>(other);
        if (x == nullptr) return false;
        //std::cout << "in key equals: name: " << name_->c_str() << " " << x->name_->c_str() <<"\n";
        bool res = this->name_->equals(x->name_) && (this->node_ == x->node_) ;
        //std::cout << res << "\n";
        return this->name_->equals(x->name_) && (this->node_ == x->node_);
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Key");
        sb->write("name_", name_);
        sb->write("node_", node_);
        sb->endSerialize();
        char* value = sb->get();
        return value;
    }
};