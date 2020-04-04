//lang: CwC
#pragma once
#include "../object.h"
#include "../utils/string.h"
#include "../serialize/jsonHelper.h"
#include "../serialize/serial.h"
#include "../store/key.h"
#include "../store/value.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/**
 * A collection of the sample classes provided in the assignment description to test serialization techniques.
 * 
 * Kept completely empty except for the field data that you'd want to serialize this file offers the easiest way to
 * see how each class would implement our serialization and deserialization techniques.
 * 
 * Unlike Array, these use the specialized constructor version of deserialization.
 */
/** authors: eldrid.s@husky.neu.edu & shetty.y@husky.neu.edu */
//A collection of the networking stuff from the assignment page that'll need serial help
enum class MsgKind { Ack=0, Nack=1, Put=2, Reply=3,  Get=4, WaitAndGet=5, Status=6, Kill=7, Register=8,  Directory=9 };

class Message : public Object {
public:
    MsgKind kind_;  // the message kind
    size_t sender_; // the index of the sender node
    size_t target_; // the index of the receiver node
    size_t id_;     // an id t unique within the node

    /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */ 
    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Message");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }
    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Message(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
    }

    Message(int kind, size_t sender, size_t target, size_t id) {
        kind_ = (enum MsgKind)kind;
        sender_ = sender;
        target_ = target;
        id_ = id;
    }

    size_t sender() {
        return sender_;
    }

    size_t target() {
        return target_;
    }

    Message() {}
};

//create all msg types. Serialize, Deserialize constructor, and add to deserialize class.

/**
 * Acknowledgement. May also be interpreted as success for something like a put. 
*/
class Ack : public Message {
public:
    Ack(size_t sender, size_t target, size_t id) {
        kind_ = (MsgKind)0; 
        sender_ = sender;
        target_ = target;
        id_ = id;
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Ack");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }

    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Ack(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
    }
};


/**
 * Negative acknowledgement. May also be interpreted as failure for something like a put. 
*/
class Nack : public Message {
public:
    Nack(size_t sender, size_t target, size_t id) {
        kind_ = (MsgKind)1; 
        sender_ = sender;
        target_ = target;
        id_ = id;
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Nack");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }

    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Nack(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
    }
};


class Put : public Message {
public:
    Key* key_;
    Value* value_;

    Put(Key* key, Value* value, size_t sender, size_t target, size_t id) {
        kind_ = (MsgKind)2; 
        sender_ = sender;
        target_ = target;
        id_ = id;
        key_ = key;
        value_ = value;
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Nack");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        char* serializedKey = key_->serialize();
        sb->write("key_", serializedKey, false);
        char* serializedValue = value_->serialize();
        sb->write("value_", serializedValue, false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }

    Put(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        key_ = new Key(JSONHelper::getValueFromKey("key_", payload)->c_str());
        value_ = new Value(JSONHelper::getValueFromKey("value_", payload)->c_str()); 
    }
};

class Get : public Message {
    Key* key_;

    Get(Key* key, size_t sender, size_t target, size_t id) {
        kind_ = (MsgKind)4; 
        sender_ = sender;
        target_ = target;
        id_ = id;
        key_ = key;
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Nack");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        char* serializedKey = key_->serialize();
        sb->write("key_", serializedKey, false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }

    Get(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        key_ = new Key(JSONHelper::getValueFromKey("key_", payload)->c_str());
    }
};

class WaitAndGet : public Message {
    Key* key_;

    WaitAndGet(Key* key, size_t sender, size_t target, size_t id) {
        kind_ = (MsgKind)5; 
        sender_ = sender;
        target_ = target;
        id_ = id;
        key_ = key;
    }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Nack");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        char* serializedKey = key_->serialize();
        sb->write("key_", serializedKey, false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }

    WaitAndGet(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        key_ = new Key(JSONHelper::getValueFromKey("key_", payload)->c_str());
    }
};


class Status : public Message {
public:
    char* status_msg_; // owned

    Status(char* status_msg, size_t sender, size_t target, size_t id) {
        kind_ = (MsgKind)6; 
        sender_ = sender;
        target_ = target;
        id_ = id;
        status_msg_ = status_msg;
    }

    /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */ 
   char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Status");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->write("msg_", status_msg_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }
    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Status(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        status_msg_ = JSONHelper::getValueFromKey("msg_", payload)->c_str();
    }
};

 

class Register : public Message {
public:
    char* client;
    size_t port_;

    /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */ 
    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Register");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->write("port", port_);
        sb->write("client", client);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }
    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Register(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        //std::cout << "payload: " << payload << "\n";
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        port_ = std::stoi(JSONHelper::getValueFromKey("port", payload)->c_str());
        client = JSONHelper::getValueFromKey("client", s)->c_str();
        //std::cout << "here\n";
    }

    Register(size_t idx, char* addr, size_t port) {
        //std::cout << "registration constructor, port: " << port <<"\n";
        kind_ = (enum MsgKind)8;
        sender_ = idx;
        target_ = 0; //assuming node 0 is server
        id_ = 0; // no indexing right now.
        client = addr;
        port_ = port;
    }
};

 

class Directory : public Message {
public:
   size_t clients;
   size_t * ports;  // owned
   String ** addresses;  // owned; strings owned

   /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */ 
   char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Directory");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->write("ports", ports, clients);
        sb->write("clients", clients);
        sb->write("addresses", addresses, clients);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }
    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Directory(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        clients = std::stoi(JSONHelper::getValueFromKey("clients", payload)->c_str());
        char* portsVals = JSONHelper::getValueFromKey("ports", payload)->c_str();
        int numPorts = JSONHelper::arrayLen(portsVals);
        size_t* tmp = new size_t[numPorts];
        for(int i = 0; i < numPorts; i++) {
            tmp[i] = std::stoi(JSONHelper::getArrayValueAt(portsVals, i)->c_str());
        }
        ports = tmp;
        char* addressesVals = JSONHelper::getValueFromKey("addresses", payload)->c_str();
        int numAddresses = JSONHelper::arrayLen(addressesVals);
        String** addresses_tmp = new String*[numAddresses];
        for(int i = 0; i < numAddresses; i++) {
            addresses_tmp[i] = JSONHelper::getArrayValueAt(addressesVals, i);
        }
        addresses = addresses_tmp;
    }

    Directory(size_t sender, size_t target, size_t id, size_t i_clients, size_t* i_ports, String** i_addresses) {
        kind_ = (enum MsgKind)9;
        sender_ = sender;
        target_ = target;
        id_ = id;
        clients = i_clients;
        size_t* tmp_ports = new size_t[i_clients];
        String** tmp_addresses = new String*[i_clients];
        for(int i = 0;  i < i_clients; i++) {
            tmp_ports[i] = i_ports[i];
        }
        ports = tmp_ports;
        for(int i = 0; i < i_clients; i++) {
            tmp_addresses[i] = i_addresses[i]->clone();
        }
        addresses = tmp_addresses; 
    }
};