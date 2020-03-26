//lang: CwC
#pragma once
#include "../object.h"
#include "../serialize/serial.h"
#include "../serialize/deserialize.h"

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

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("KeyVal");
        char * seralizedKey = key_->serialize();
        sb->write("key_", seralizedKey, false);
        char * seralizedVal = val_->serialize();
        sb->write("val_", seralizedVal, false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }

    static KeyVal* deserialize(char* s) {
        Deserializable* ds = new Deserializable();
        char* key_serialized = JSONHelper::getValueFromKey("key_", s)->c_str();
        Object* key = ds->deserialize(key_serialized);
        Object* val = ds->deserialize(JSONHelper::getValueFromKey("val_", s)->c_str());
        KeyVal* kv = new KeyVal(key, val);
        return kv;
    }
};