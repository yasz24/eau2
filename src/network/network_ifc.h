#include "../object.h"
#include "network.h"

class NetworkIfc : public Object {
public:
    virtual void register_node(size_t idx) {};
    //return the index of the node.
    virtual size_t index() { assert(false); };
    //send the message, msg is consumed.
    virtual void send_msg(Message* msg) = 0;
    //wait for a message to be received. Message becomes owned.
    virtual Message* recv_msg() = 0;
};