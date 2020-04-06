//code adapted from jan vitek's networking video
#include "network_ip.h"
#include <iostream>
#define PORT 3000
#define SERVER_IP "127.0.0.1"

//authors: shetty.y@husky.neu.edu, eldrid.s@husky.neu.edu

int main(int argc, char *argv[]) 
{ 
	char *ip;
	size_t port;
	size_t node_idx;

	if (argc != 5  || (strcmp(argv[1], "-ip") != 0) || (strcmp(argv[3], "-idx") != 0)) {
	    fprintf(stderr,"usage: client -ip <host-ip-address>:PORT -idx <node_idx>\n");
	    _exit(1);
	} 
	ip = strtok(argv[2], ":");
    port = std::stoi(strtok(NULL, ":"));
	node_idx = std::stoi(argv[4]);

	std::cout<< "port is: "<<port << "\n";
	NetworkIP* client_ = new NetworkIP();

	client_->client_init(node_idx, ip, port, SERVER_IP, PORT);
	return 0; 
} 
