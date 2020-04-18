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

    Key(String* name) {
        this->name_ = name;
        this->node_ = 0;
    }

    Key(char* serialized) {
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->name_ = new String(JSONHelper::getValueFromKey("name_", payload)->c_str());
        this->node_ = std::stoi(JSONHelper::getValueFromKey("node_", payload)->c_str());
    }

    size_t node() {
        return this->node_;
    }

    String* name() {
        return this->name_;
    }

    size_t hash_me() {
        //std::cout << this->name_->c_str() << "\n";
        size_t _hash = 0;
        _hash += this->name_->hash();
        //std::cout << "Key hash: " << _hash << "\n";
    }

    bool equals(Object* other) {
        if (other == this) return true;
        Key* x = dynamic_cast<Key *>(other);
        if (x == nullptr) return false;
        //std::cout << "in key equals: name: " << name_->c_str() << " " << x->name_->c_str() <<"\n";
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
    //added for linus
    Key* clone() {
        Key* temp = new Key(name_->clone());
        temp->node_ = node_;
        return temp;
    }
};

struct KeyCompare {
    bool operator() (const Key* k1, const Key* k2) const {
       if (strcmp(k1->name_->c_str(), k2->name_->c_str()) < 0) {
           return true;
       }
       return false;
   }
};