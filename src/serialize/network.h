//lang: CwC
#pragma once
#include "../object.h"
#include "string.h"
#include "jsonHelper.h"
#include "serial.h"
#include <string>

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

    Message() {}
};

 

class Ack : public Message {
public:
};

 

class Status : public Message {
public:
   String* msg_; // owned

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
        sb->write("msg_", msg_);
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
        msg_ = JSONHelper::getValueFromKey("msg_", payload);
    }
};

 

class Register : public Message {
public:
    size_t client;
    size_t port;

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
        sb->write("port", port);
        sb->write("client", client);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }
    //Special constructor that given a serialized representation of an object of this class, generates a new one with the same data
    Register(char* s) {
        char* payload = JSONHelper::getPayloadValue(s)->c_str();
        kind_ = (enum MsgKind)std::stoi(JSONHelper::getValueFromKey("kind_", payload)->c_str());
        sender_ = std::stoi(JSONHelper::getValueFromKey("sender_", payload)->c_str());
        target_ = std::stoi(JSONHelper::getValueFromKey("target_", payload)->c_str());
        id_ = std::stoi(JSONHelper::getValueFromKey("id_", payload)->c_str());
        port = std::stoi(JSONHelper::getValueFromKey("port", payload)->c_str());
        client = std::stoi(JSONHelper::getValueFromKey("client", s)->c_str());
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
        int portsLen = sizeof(&ports)/sizeof(ports[0]);
        int addressLen = sizeof(&addresses)/sizeof(addresses[0]);
        sb->initSerialize("Message");
        sb->write("kind_", (int)kind_);
        sb->write("sender_", sender_);
        sb->write("target_", target_);
        sb->write("id_", id_);
        sb->write("ports", ports, portsLen);
        sb->write("clients", clients);
        sb->write("addresses", addresses, addressLen);
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
        ports = new size_t[numPorts];
        for(int i = 0; i < numPorts; i++) {
            ports[i] = std::stoi(JSONHelper::getArrayValueAt(portsVals, i)->c_str());
        }
        char* addressesVals = JSONHelper::getValueFromKey("addresses", payload)->c_str();
        int numAddresses = JSONHelper::arrayLen(addressesVals);
        addresses = new String*[numAddresses];
        for(int i = 0; i < numAddresses; i++) {
            addresses[i] = JSONHelper::getArrayValueAt(addressesVals, i);
        }
    }

    Directory(int kind, int sender, int target, int id, size_t i_clients, size_t* i_ports, String** i_addresses) {
        kind_ = (enum MsgKind)kind;
        sender_ = sender;
        target_ = target;
        id_ = id;
        clients = i_clients;
        int portsLen = sizeof(*i_ports)/sizeof(i_ports[0]);
        ports = new size_t[portsLen];
        int addressesLen = sizeof(i_addresses)/sizeof(i_addresses[0]);
        addresses = new String*[addressesLen];
        for(int i = 0;  i < portsLen; i++) {
            ports[i] = i_ports[i];
        }
        for(int i = 0; i < addressesLen; i++) {
            addresses[i] = i_addresses[i]->clone();
        }
    }
};