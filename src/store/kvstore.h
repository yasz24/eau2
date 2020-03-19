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

    void put(Key* key, Value* value) {
        size_t node = key->node();
        if (node == this->this_node_) {
            this->local_store_->add(key, value);
        } else {
            //networking. dispatch and put in appropriate node.
        }
    }

    Value* get(Key* key) {
        size_t node = key->node();
        if (node == this->this_node_) {
            return dynamic_cast<Value *>(this->local_store_->get(key));
        } else {
            //networking. dispatch request and get from appropriate node.
        }
    }

    Value* waitAndget(Key* key) {
        size_t node = key->node();
        if (node == this->this_node_) {
            //while loop?. might stack overflow. could sleep and try at regular intervals.
        } else {
            //networking. dispatch request and get from appropriate node.
        }
    }
};