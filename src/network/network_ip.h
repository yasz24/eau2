#include "../object.h"
#include "../utils/string.h"
#include "network_ifc.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

class NodeInfo : public Object {
public:
    size_t id;
    sockaddr_in address;
};

class NetworkIP : public NetworkIfc {
public:
    NodeInfo* nodes_;
    size_t num_nodes_;
    size_t this_node_;
    int sock_;
    sockaddr_in ip_;

    ~NetworkIP() {
        close(sock_);
    }

    size_t index() override {
        return this_node_;
    }

    void server_init(size_t idx, sockaddr_in ip, size_t port, size_t num_nodes) {
        this_node_ = idx;
        init_sock_(port);
        num_nodes_ = num_nodes;
        nodes_ = new NodeInfo[num_nodes];
        for (size_t i = 0; i < num_nodes_; i++) nodes_[i].id = 0;
        ip_ = ip;
        nodes_[0].address = ip;
        nodes_[0].id = 0;
        for (size_t i = 1; i < num_nodes; i++) {
            Register* msg = dynamic_cast<Register*>(recv_msg());
            nodes_[msg->sender()].id = msg->sender();
            nodes_[msg->sender()].address.sin_family = AF_INET;
            nodes_[msg->sender()].address.sin_addr = msg->client.sin_addr;
            nodes_[msg->sender()].address.sin_port = htons(msg->port);
        }
        size_t* ports = new size_t[num_nodes_ - 1];
        String** addresses = new String*[num_nodes_ - 1];
        for (size_t i = 0; i < num_nodes_ - 1; i++) {
            ports[i] = ntohs(nodes_[i + 1].address.sin_port);
            addresses[i] = new String(inet_ntoa(nodes_[i + 1].address.sin_addr));
        }
        Directory ipd(this_node_, -1, ports, addresses)
        
        
    }

    void init_sock_(size_t port) {

    }

    void send_msg
};