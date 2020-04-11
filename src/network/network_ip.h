#pragma once
#include "../object.h"
#include "../utils/string.h"
#include "../serialize/serial.h"
#include "network_ifc.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "../serialize/deserialize.h"

class NodeInfo : public Object {
public:
    size_t id;
    sockaddr_in address;
};

class NetworkIP : public NetworkIfc {
public:
    NodeInfo* nodes_;
    size_t num_nodes_ = 1;
    size_t this_node_ = 0;
    int sock_;
    sockaddr_in ip_;
    size_t msg_id = 0;

    ~NetworkIP() {
        close(sock_);
    }

    size_t index() override {
        return this_node_;
    }

    void server_init(size_t idx, char* ip, size_t port, size_t num_nodes) {
        this_node_ = idx;
        init_sock_(ip, port);
        std::cout << "initialized socket, listening on " << ip << ":" << port << "\n";
        num_nodes_ = num_nodes;
        nodes_ = new NodeInfo[num_nodes];
        for (size_t i = 0; i < num_nodes_; i++) nodes_[i].id = 0;
        nodes_[0].address = ip_;
        nodes_[0].id = 0;
        for (size_t i = 1; i < num_nodes; i++) {
            Register* msg = dynamic_cast<Register*>(recv_msg());
            //std::cout  << "Registration msg received, " << "client: " << msg->client << ":" << msg->port_ <<"\n";
            nodes_[msg->sender()].id = msg->sender();
            nodes_[msg->sender()].address.sin_family = AF_INET;
            inet_pton(AF_INET, msg->client, &nodes_[msg->sender()].address.sin_addr);
            nodes_[msg->sender()].address.sin_port = htons(msg->port_);
        }
        size_t *ports = new size_t[num_nodes_];
        String** addresses = new String*[num_nodes_];
        for (size_t i = 0; i < num_nodes_; i++) {
            ports[i] = ntohs(nodes_[i].address.sin_port);
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &nodes_[i].address.sin_addr, client_ip, INET_ADDRSTRLEN);
            addresses[i] = new String(client_ip);
        }
        Directory ipd(this_node_, 0, msg_id, num_nodes_, ports, addresses);

        for (size_t i = 1; i < num_nodes_; i++) {
            ipd.target_ = i;
            send_msg(&ipd);
        }
    }

    void client_init(size_t idx, char* ip, size_t port, char* server_adr, size_t server_port) {
        this_node_ = idx;
        init_sock_(ip, port);
        nodes_ = new NodeInfo[1];
        nodes_[0].id = 0;
        nodes_[0].address.sin_family = AF_INET;

        nodes_[0].address.sin_addr.s_addr = inet_addr(ip);
        nodes_[0].address.sin_port = htons(server_port);
        
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ip_.sin_addr, client_ip, INET_ADDRSTRLEN);
        std::cout << "creating registration msg, port: " << port <<"\n";
        Register reg(this_node_, 0, msg_id, client_ip, port);

        send_msg(&reg);

        Directory* ipd = dynamic_cast<Directory*>(recv_msg());
        num_nodes_ = ipd->clients;
        NodeInfo * nodes = new NodeInfo[num_nodes_];
        for (size_t i = 0; i < num_nodes_; i++) {
            char* node_ip = ipd->addresses[i]->c_str();
            size_t node_port = ipd->ports[i];
            nodes[i].id = i;
            nodes[i].address.sin_family = AF_INET;
            if (inet_pton(AF_INET, node_ip, &nodes[i].address.sin_addr) < 0) {
                perror("Node storage:");
            }
            nodes[i].address.sin_port = htons(node_port);
            int decoded_port = ntohs(nodes[i].address.sin_port);
            char decoded_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &nodes[i].address.sin_addr, decoded_ip, INET_ADDRSTRLEN);
            std::cout << "Directory: node " << i << ", ip: " <<  decoded_ip << ", port: " << decoded_port << "\n";
            //nodes[i].address.sin_addr.s_addr = inet_addr(node_ip);
        }
        nodes_ = nodes;
        delete ipd;
    }

    void init_sock_(char* ip, size_t port) {
        assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
        int opt =1;
        assert(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt) == 0));
        ip_.sin_family = AF_INET;
        //ip_.sin_addr.s_addr = inet_addr(ip);
        inet_pton(AF_INET, ip, &ip_.sin_addr);
        ip_.sin_port = htons(port);
        //int res = bind(sock_, (sockaddr*) &ip_, sizeof(ip_));
        if( bind(sock_, (sockaddr*) &ip_, sizeof(ip_)) < 0){
            perror("bind failed. Error");
            return;
        }
        if( (listen(sock_, 100)) < 0){
            perror("listen failed. Error");
            return;
        }
    }

    void send_msg(Message* msg) {
        size_t target_node = msg->target();
        std::cout << "send msg target: " << target_node << "\n";
        NodeInfo &target = nodes_[target_node];
        int server_port = ntohs(target.address.sin_port);
        char server_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &target.address.sin_addr, server_ip, INET_ADDRSTRLEN);
        std::cout << "attempting to send msg to: " << server_ip << ":" << server_port << "\n";
        int connection = socket(AF_INET, SOCK_STREAM, 0);
        assert(connection >= 0);
        if (connect(connection, (sockaddr*)&target.address, sizeof(target.address)) < 0) {
            perror("connection failed. Error");
            return;
        }
        msg->id_ = msg_id;
        msg_id++;
        //assert(connect(connection, (sockaddr*)&target.address, sizeof(target.address)) >= 0);
        char* serialized = msg->serialize();
        std::cout << "sending msg: " << serialized << "\n";
        size_t length = strlen(serialized);
        send(connection, &length, sizeof(size_t), 0);
        send(connection, serialized, length, 0);
        close(connection);
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
        std::cout << "received msg: " << buf << "\n";
        Deserializable ds;
        Message* msg = dynamic_cast<Message*>(ds.deserialize(buf));
        close(req);
        return msg;
    }
};