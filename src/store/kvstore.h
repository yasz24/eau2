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
            Message* m = network_->recv_msg(); //wait to receive resp. Ack or Nack.
            msg_consumed = true; //indicate to listening thread that the incoming message has been consumed.
            //expect Ack or Nack
            if (m->kind_ == MsgKind::Nack) {
                std::cout<< "put on " << target << "node failed\n";
                res = false;
            } else {
                assert((m->kind_ == MsgKind::Ack)); //just to test. take out
            }
        }
        store_mtx_.unlock();
        return res;
    }

    Value* get(Key* key) {
        store_mtx_.lock();
        // std::cout << "calling get\n";
        size_t target = key->node();
        //std::cout << "get: " << "key: " << key->serialize() << "\n";
        Value* val = nullptr;
        if (target == this->this_node_) {
            std::map<Key*, Value*, KeyCompare>::iterator iter;
            iter = local_store_.find(key);
            if (iter != local_store_.end()) {
                val = iter->second;
            }
        } else {
            Get get(key, this_node_, target, network_->msg_id);
            network_->send_msg(&get); // send get msg over the network to appropriate KVStore.
            Message* m = network_->recv_msg(); //wait to receive resp. Reply or Nack.
            msg_consumed = true; //indicate to listening thread that the incoming message has been consumed.
            //expect Reply or Nack.
            if (m->kind_ == MsgKind::Reply) {
                Reply* resp = dynamic_cast<Reply*>(m);
                char* serializedValue = resp->reply_msg_;
                val = new Value(serializedValue);
            } else if (m->kind_ == MsgKind::Nack) {
                std::cout << "unable to get key from node " << target <<"\n";
            }
        }
        store_mtx_.unlock();
        return val;
    }

    Value* waitAndget(Key* key) {
        store_mtx_.lock();
        size_t node = key->node();
        Value* val = nullptr;
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
            store_mtx_.lock(); //acquire the lock so we're the only ones accepting on the network_ sock or modifying local store.

            if (FD_ISSET(listener, &read_fds) && !(msg_consumed)) {
                Message* request = network_->recv_msg();
                process_request_(request);
            } 
            store_mtx_.unlock();
        }
    }

    //figure out the type of request and act accordingly.
    void process_request_(Message* m) {
        MsgKind kind = m->kind_;

        //switch on the kind and call the appropriate processing function.
        switch (kind) {
        case MsgKind::Put: {
            Put* _put = dynamic_cast<Put*>(m);
            process_put_request_(_put);
            break;
        }
        case MsgKind::Get: {
            Get* _get = dynamic_cast<Get*>(m);
            process_get_request_(_get);
            break;
        }
        case MsgKind::WaitAndGet: {
            WaitAndGet* _waitAndget = dynamic_cast<WaitAndGet*>(m);
            process_waitAndGet_request_(_waitAndget);
            break;
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