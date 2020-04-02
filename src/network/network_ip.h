#include "../object.h"
#include "../utils/string.h"
#include "../serialize/serial.h"
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

    void server_init(size_t idx, size_t port, size_t num_nodes) {
        this_node_ = idx;
        init_sock_(port);
        num_nodes_ = num_nodes;
        nodes_ = new NodeInfo[num_nodes];
        for (size_t i = 0; i < num_nodes_; i++) nodes_[i].id = 0;
        nodes_[0].address = ip_;
        nodes_[0].id = 0;
        for (size_t i = 1; i < num_nodes; i++) {
            Register* msg = dynamic_cast<Register*>(recv_msg());
            nodes_[msg->sender()].id = msg->sender();
            nodes_[msg->sender()].address.sin_family = AF_INET;
            inet_pton(AF_INET, msg->client, &nodes_[msg->sender()].address.sin_addr);
            nodes_[msg->sender()].address.sin_port = htons(msg->port);
        }
        size_t* ports = new size_t[num_nodes_];
        String** addresses = new String*[num_nodes_];
        for (size_t i = 0; i < num_nodes_; i++) {
            ports[i] = ntohs(nodes_[i].address.sin_port);
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &nodes_[i].address.sin_addr, client_ip, INET_ADDRSTRLEN);
            addresses[i] = new String(client_ip);
        }
        Directory ipd(this_node_, -1, num_nodes_, ports, addresses);

        for (size_t i = 1; i < num_nodes_; i++) {
            ipd.target_ = i;
            send_msg(&ipd);
        }
    }

    void client_init(size_t idx, size_t port, char* server_adr, size_t server_port, size_t num_nodes) {
        this_node_ = idx;
        num_nodes_ = num_nodes;
        init_sock_(port);
        nodes_ = new NodeInfo[1];
        nodes_[0].id = 0;
        nodes_[0].address.sin_family = AF_INET;
        nodes_[0].address.sin_port = htons(server_port);
        assert((inet_pton(AF_INET, server_adr, &nodes_[0].address.sin_addr) > 0));
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ip_.sin_addr, client_ip, INET_ADDRSTRLEN);
        Register reg(idx, client_ip, port);

        send_msg(&reg);

        Directory* ipd = dynamic_cast<Directory*>(recv_msg());
        NodeInfo * nodes = new NodeInfo[num_nodes];
        for (size_t i = 0; i < ipd->clients; i++) {
            nodes[i].id = i;
            nodes[i].address.sin_family = AF_INET;
            nodes[i].address.sin_port = htons(ipd->ports[i]);
            assert((inet_pton(AF_INET, ipd->addresses[i]->c_str(), &nodes[i].address.sin_addr)) > 0);
        }
        delete[] nodes;
        nodes_ = nodes;
        delete ipd;
    }

    void init_sock_(size_t port) {
        assert((sock_ = socket(AF_INET, SOCK_STREAM, 0) >= 0));
        int opt =1;
        assert(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt) == 0));
        ip_.sin_family = AF_INET;
        ip_.sin_addr.s_addr = INADDR_ANY;
        ip_.sin_port = htons(port);
        assert(bind(sock_, (sockaddr*) &ip_, sizeof(ip_)) >= 0);
        assert((listen(sock_, 100) >= 0)); // connection queue size up to 100
    }

    void send_msg(Message* msg) {
        NodeInfo &target = nodes_[msg->target()];
        int connection = socket(AF_INET, SOCK_STREAM, 0);
        assert(connect >= 0);
        assert(connect(connection, (sockaddr*)&target.address, sizeof(target.address)) >= 0);
        char* serialized = msg->serialize();
        size_t length = strlen(serialized);
        send(connection, &length, sizeof(size_t), 0);
        send(connection, serialized, length, 0);
    }

    Message* recv_msg() {
        sockaddr_in sender;
        socklen_t addrlen = sizeof(sender);
        int req = accept(sock_, (sockaddr*)&sender, &addrlen);
        size_t size = 0;
        read(req, &size, sizeof(size_t));
        char* buf = new char[size];
        int rd = 0;
        while (rd != size) {
            rd+= read(req, buf + rd, size - rd);
        }
        Message* msg = new Message(buf);
        return msg;
    }
};