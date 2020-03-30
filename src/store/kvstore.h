#pragma once
#include "../object.h"
#include "key.h"
#include "value.h"
#include "../utils/map.h"

class KVStore : public Object {
public:
    size_t num_nodes_;
    size_t this_node_;
    Map* local_store_; // Map<Key, Value>, owned. contains all the key's that are supposed to be on the local store.

    KVStore(size_t num_nodes, size_t this_node) {
        this->num_nodes_ = num_nodes;
        this->this_node_ = this_node;
        this->local_store_ = new Map();
    }

    KVStore(char* serialized) {
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->num_nodes_ = std::stoi(JSONHelper::getValueFromKey("num_nodes_", payload)->c_str());
        this->this_node_ = std::stoi(JSONHelper::getValueFromKey("this_node_", payload)->c_str());
        this->local_store_ = new Map(JSONHelper::getValueFromKey("local_store_", payload)->c_str());
        //std::cout<<JSONHelper::getValueFromKey("local_store_", payload)->c_str()<<"\n";
    }

    void put(Key* key, Value* value) {
        //std::cout << "put: " << "key: " << key->serialize() << " val: " << value->serialize() << "\n";
        size_t node = key->node();
        if (node == this->this_node_) {
            this->local_store_->add(key, value);
            //std::cout << "in put\n";
        } else {
            //networking. dispatch and put in appropriate node.
            std::cout<<"ERROR: ENCOUNTERED NETWORKING CODE IN KVSTORE-PUT\n";
        }
    }

    Value* get(Key* key) {
        // std::cout << "calling get\n";
        size_t node = key->node();
        //std::cout << "get: " << "key: " << key->serialize() << "\n";
        if (node == this->this_node_) {
            Value* val = dynamic_cast<Value*>(this->local_store_->get(key));
            return val;
        } else {
            //networking. dispatch request and get from appropriate node.
            std::cout<<"ERROR: ENCOUNTERED NETWORKING CODE IN KVSTORE-GET\n";
            return nullptr;
        }
    }

    Value* waitAndget(Key* key) {
        size_t node = key->node();
        if (node == this->this_node_) {
            //while loop?. might stack overflow. could sleep and try at regular intervals.
        } else {
            //networking. dispatch request and get from appropriate node.
            std::cout<<"ERROR: ENCOUNTERED NETWORKING CODE IN KVSTORE-WAITANDGET\n";
            return nullptr;
        }
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("KVStore");
        sb->write("num_nodes_", num_nodes_);
        sb->write("this_node_", this_node_);
        //std::cout<<local_store_->serialize()<<"\n";
        sb->write("local_store_", local_store_->serialize(), false);
        sb->endSerialize();
        char* value = sb->get();
        return value;
    }
};