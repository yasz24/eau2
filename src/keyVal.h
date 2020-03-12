//lang: CwC
#pragma once
#include "object.h"
//authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
/**
 * A simple object that represents a key-value pair.
*/
class KeyVal : public Object {
public:
    Object *key_;
    Object *val_;

    /**
     * Constructor.
    */
    KeyVal(Object *key, Object *val) {
        //check if key or val are null ptr.
        this->key_ = key;
        this->val_ = val;
    }

    Object* getKey() {
        return this->key_;
    }

    Object* getVal() {
        return this->val_;
    }

    virtual bool equals(Object *other) {
        if (other == nullptr) return false;
		KeyVal *kv = dynamic_cast<KeyVal*>(other);
		if (kv == nullptr) {
			return false;
		}
        //it is okay to only check if the keys because in a map one key only maps to a single value. 
		return this->getKey()->equals(kv->getKey());
	}

    size_t hash() {
        return this->key_->hash() + this->val_->hash();
    }
};