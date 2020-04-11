#pragma once
#include "../object.h"
#include "../serialize/serial.h"
#include "../serialize/deserialize.h"
#include "../serialize/jsonHelper.h"
//todo: write hash, equals
class Value : public Object{
public:
    char* data; //owned
    size_t length; 

    Value(char* data, size_t length) {
        this->data = data;
        this->length = length;
    }

    Value(char* serialized) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        char* serialData = JSONHelper::getValueFromKey("data", payload)->c_str();
        this->data = serialData;
        if(JSONHelper::isObject(serialData)) {
            this->data = ds->deserialize(JSONHelper::getValueFromKey("data", payload)->c_str())->serialize();
        } else {
            this->data = serialData;
        }
        this->length = std::stoi(JSONHelper::getValueFromKey("length", payload)->c_str());
    }

     Value(char* serialized, KVStore* kv) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        char* serialData = JSONHelper::getValueFromKey("data", payload)->c_str();
        this->data = serialData;
        if(JSONHelper::isObject(serialData)) {
            this->data = ds->deserialize(serialData, kv)->serialize();
        } else {
            this->data = serialData;
        }
        this->length = std::stoi(JSONHelper::getValueFromKey("length", payload)->c_str());
    }


    size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < strlen(data); ++i) {
            hash += data[i];
        }
        return hash;
    }

    bool equals(Object* other) {
        if (other == this) return true;
        Value* x = dynamic_cast<Value *>(other);
        if (x == nullptr) return false;
        if (this->length != x->length) return false;
        return strncmp(data, x->data, length) == 0;
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Value");
        if(JSONHelper::isObject(data)) {
            sb->write("data", data, false);
        } else {
            sb->write("data", data);
        }
        sb->write("length", length);
        sb->endSerialize();
        char* value = sb->get();
        return value;
    }
};