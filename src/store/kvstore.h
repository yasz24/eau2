#pragma once
#include "../object.h"
#include "key.h"
#include "value.h"
#include "../utils/map.h"
#include "../network/network_ip.h"
#include <map>
#include <thread>
#include <mutex>

class KVStore : public Object {
public:
    size_t num_nodes_;
    size_t this_node_;
    std::map<Key*, Value*, KeyCompare> local_store_; // Map<Key, Value>, owned. contains all the key's that are supposed to be on the local store.
    NetworkIP* network_;
    std::mutex store_mtx_;
    std::thread listening_thread_;
    bool msg_consumed = false;

    KVStore(size_t num_nodes, size_t this_node, NetworkIP* network) {
        this->num_nodes_ = num_nodes;
        this->this_node_ = this_node;
        network_ = network;
    }

    KVStore(char* serialized) {
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->num_nodes_ = std::stoi(JSONHelper::getValueFromKey("num_nodes_", payload)->c_str());
        this->this_node_ = std::stoi(JSONHelper::getValueFromKey("this_node_", payload)->c_str());
    }

    void put(Key* key, Value* value) {
        store_mtx_.lock();
        //std::cout << "put: " << "key: " << key->serialize() << " val: " << value->serialize() << "\n";
        size_t node = key->node();
        if (node == this->this_node_) {
            local_store_.insert(std::pair<Key*, Value*>(key, value));
            //std::cout << "in put\n";
        } else {

            //send_msg();
            //recv_msg();
            //networking. dispatch and put in appropriate node.
            std::cout<<"ERROR: ENCOUNTERED NETWORKING CODE IN KVSTORE-PUT\n";
        }
        store_mtx_.unlock();
    }

    Value* get(Key* key) {
        store_mtx_.lock();
        // std::cout << "calling get\n";
        size_t node = key->node();
        //std::cout << "get: " << "key: " << key->serialize() << "\n";
        Value* val;
        if (node == this->this_node_) {
            val = local_store_.find(key)->second;
        } else {
            //networking. dispatch request and get from appropriate node.
            std::cout<<"ERROR: ENCOUNTERED NETWORKING CODE IN KVSTORE-GET\n";
            return nullptr;
        }
        store_mtx_.unlock();
        return val;
    }

    Value* waitAndget(Key* key) {
        store_mtx_.lock();
        size_t node = key->node();
        Value* val;
        if (node == this->this_node_) {
            //while loop?. might stack overflow. could sleep and try at regular intervals.
        } else {
            //networking. dispatch request and get from appropriate node.
            std::cout<<"ERROR: ENCOUNTERED NETWORKING CODE IN KVSTORE-WAITANDGET\n";
        }
        store_mtx_.unlock();
        return val;
    }

    void start() {
        listening_thread_ = std::thread(&KVStore::listen, this);
    }

    //think about case when recv calls accept first, because select would still unblock i think.
    void listen() {
        fd_set read_fds;
        int listener = network_->sock_;
        FD_ZERO(&read_fds);
        FD_SET(listener, &read_fds);

        //listen for any incoming requests to the store.
        while (true) {
            msg_consumed = false;
            //wait till we read some activity on the listener.
            if (select(listener + 1, &read_fds, NULL, NULL, NULL) == -1) {
                perror("select");
                return;
            }
            store_mtx_.lock(); //acquire the lock so we're the only accepting the network sock or modifying local store.

            if (FD_ISSET(listener, &read_fds) && !(msg_consumed)) {
                Message* request = network_->recv_msg();
                process_request_(request);
            } 
            store_mtx_.unlock();
        }
    }

    //figure out the type of request and act accordingly.
    void process_request_(Message* m) {

    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("KVStore");
        sb->write("num_nodes_", num_nodes_);
        sb->write("this_node_", this_node_);
        sb->endSerialize();
        char* value = sb->get();
        return value;
    }
};