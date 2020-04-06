#pragma once
#include "../object.h"
#include "key.h"
#include "value.h"
#include "../utils/map.h"
#include "../network/network.h"
#include "../network/network_ip.h"
#include <map>
#include <thread>
#include <mutex>
#include "../utils/lock.h"
#include "../network/network_ip.h"
class KVStore : public Object {
public:
    size_t num_nodes_;
    size_t this_node_;
    std::map<Key*, Value*, KeyCompare> local_store_; // Map<Key, Value>, owned. contains all the key's that are supposed to be on the local store.
    NetworkIP* network_;
    Lock store_mtx_;
    Message* received_msg;
    Array pendingGets;
    Key* wait_for_key_;
    std::thread listening_thread_;
    bool msg_consumed = false;

    //Network Enabled Constructor
    KVStore(size_t num_nodes, size_t this_node, NetworkIP* network) {
        this->num_nodes_ = num_nodes;
        this->this_node_ = this_node;
        network_ = network;
    }

    //Legacy Non networked Constructor
    KVStore(size_t num_nodes, size_t this_node) {
        this->num_nodes_ = num_nodes;
        this->this_node_ = this_node;
        //this->local_store_ = new Map();
    }

    KVStore(char* serialized) {
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->num_nodes_ = std::stoi(JSONHelper::getValueFromKey("num_nodes_", payload)->c_str());
        this->this_node_ = std::stoi(JSONHelper::getValueFromKey("this_node_", payload)->c_str());
    }

    bool put(Key* key, Value* value) {
        store_mtx_.lock(); //acquire lock ownership
        //std::cout << "put: " << "key: " << key->serialize() << " val: " << value->serialize() << "\n";
        size_t target = key->node();
        bool res = true;
        if (target == this->this_node_) {
            local_store_.insert(std::pair<Key*, Value*>(key, value));
        } else {
            Put put(key, value, this_node_, target, network_->msg_id);
            network_->send_msg(&put); // send put msg over the network to appropriate KVStore.
            store_mtx_.wait();
            Message* m = received_msg; //wait to receive resp. Ack or Nack.
            //expect Ack or Nack
            if (m->kind_ == MsgKind::Nack) {
                std::cout<< "put on " << target << "node failed\n";
                res = false;
            }  
        }
        // notification procedure.
        resolve_local_wait(key);
        //loop through pendingGets, and resolve any that should be.
        resolve_remote_wait(key, value);
        store_mtx_.unlock();
        return res;
    }

    void resolve_local_wait(Key* key) {
         // notification procedure.
        if (wait_for_key_ != nullptr) {
            if (wait_for_key_->equals(key)) {
                store_mtx_.notify_all();
            }
        }
    }

    void resolve_remote_wait(Key* key, Value* val) {
        size_t outstandingReqs = pendingGets.length();
        if (outstandingReqs > 0) {
            for (size_t i = 0; i < outstandingReqs; i++) {
                WaitAndGet* req = dynamic_cast<WaitAndGet*>(pendingGets.get(i));
                if (req->key_->equals(key)) {
                    //remove the req from pending reqs list.
                    pendingGets.remove(i);
                    //compose a response to the sender
                    Reply resp(val->serialize(), this_node_, req->sender_, network_->msg_id);
                    //send the message
                    network_->send_msg(&resp);
                }   
            }
        }
    }

    Value* get(Key* key) {
        store_mtx_.lock();
        size_t target = key->node();
        //std::cout << "get: " << "key: " << key->serialize() << "\n";
        Value* val = nullptr;
        if (target == this_node_) {
            std::map<Key*, Value*, KeyCompare>::iterator iter;
            iter = local_store_.find(key);
            if (iter != local_store_.end()) {
                val = iter->second;
            }
        } else {
            Get get(key, this_node_, target, network_->msg_id);
            network_->send_msg(&get); // send get msg over the network to appropriate KVStore.
            store_mtx_.wait();
            Message* m = received_msg;
            //expect Reply or Nack.
            if (m->kind_ == MsgKind::Reply) {
                Reply* resp = dynamic_cast<Reply*>(m);
                char* serializedValue = resp->reply_msg_;
                val = new Value(serializedValue);
            } else if (m->kind_ == MsgKind::Nack) {
                std::cout << "unable to get key from node " << target <<"\n";
            }
            received_msg = nullptr;
        }
        store_mtx_.unlock();
        return val;
    }

    Value* waitAndget(Key* key) {
        store_mtx_.lock();
        size_t node = key->node();
        Value* val = nullptr;
        if (node == this->this_node_) {
            std::map<Key*, Value*, KeyCompare>::iterator iter;
            iter = local_store_.find(key);
            if (iter != local_store_.end()) {
                val = iter->second;
            } else {
                wait_for_key_ = key;
                store_mtx_.wait();
                iter = local_store_.find(key);
                val = iter->second;
                wait_for_key_ = nullptr;
            }
        } else {
            WaitAndGet get(key, this_node_, node, network_->msg_id);
            network_->send_msg(&get); // send get msg over the network to appropriate KVStore.
            store_mtx_.wait();
            Message* m = received_msg;
            //expect Reply. 
            if (m->kind_ == MsgKind::Reply) {
                Reply* resp = dynamic_cast<Reply*>(m);
                char* serializedValue = resp->reply_msg_;
                val = new Value(serializedValue);
            } 
            received_msg = nullptr;
        }
        store_mtx_.unlock();
        return val;
    }

    void start() {
        listening_thread_ = std::thread(&KVStore::listen, this);
    }

    //think about case when recv calls accept first, because select would still unblock i think.
    void listen() {
        //listen for any incoming requests to the store.
        while (true) {
            Message* message = network_->recv_msg();
            process_message_(message);
        }
    }

    //figure out the type of request and act accordingly.
    void process_message_(Message* m) {
        MsgKind kind = m->kind_;

        //switch on the kind and call the appropriate processing function.
        switch (kind) {
        case MsgKind::Put: {
            Put* put = dynamic_cast<Put*>(m);
            process_put_request_(put);
            break;
        }
        case MsgKind::Get: {
            Get* get = dynamic_cast<Get*>(m);
            process_get_request_(get);
            break;
        }
        case MsgKind::WaitAndGet: {
            WaitAndGet* waitAndget = dynamic_cast<WaitAndGet*>(m);
            process_waitAndGet_request_(waitAndget);
            break;
        }
        case MsgKind::Reply: {
            store_mtx_.lock();
            Reply* reply = dynamic_cast<Reply*>(m);
            received_msg = reply;
            store_mtx_.notify_all();
            store_mtx_.unlock();
        }
        case MsgKind::Ack: {
            store_mtx_.lock();
            Ack* reply = dynamic_cast<Ack*>(m);
            received_msg = reply;
            store_mtx_.notify_all();
            store_mtx_.unlock();
        }
        case MsgKind::Nack: {
            store_mtx_.lock();
            Nack* reply = dynamic_cast<Nack*>(m);
            received_msg = reply;
            store_mtx_.notify_all();
            store_mtx_.unlock();
        }
        default:
            break;
        }
    }

    //don't need to lock, because subroutine of process_request_
    void process_put_request_(Put* _put) {
        if (put(_put->key_, _put->value_)) {
            Ack ack(this_node_, _put->sender_, network_->msg_id);
            network_->send_msg(&ack);
        } else {
            Nack nack(this_node_, _put->sender_, network_->msg_id);
            network_->send_msg(&nack);
        }
    }

    void process_get_request_(Get* _get) {
        Value* val = get(_get->key_);
        if (val != nullptr) {
            Reply res(val->serialize(), this_node_, _get->sender_, network_->msg_id);
            network_->send_msg(&res);
        } else {
            Nack nack(this_node_, _get->sender_, network_->msg_id);
            network_->send_msg(&nack);
        }
    }

    void process_waitAndGet_request_(WaitAndGet* _waitAndget) {
        Value* val = get(_waitAndget->key_);
        if (val != nullptr) {
            Reply res(val->serialize(), this_node_, _waitAndget->sender_, network_->msg_id);
            network_->send_msg(&res);
        } else {
            pendingGets.pushBack(_waitAndget);
        }
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