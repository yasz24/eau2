//code adapted from jan vitek's networking video
#include "network_ip.h"

//authors: shetty.y@husky.neu.edu, eldrid.s@husky.neu.edu

#define PORT "8888"   // port we're listening on

int main(int argc , char *argv[]) {
	char *ip;
	size_t port;
	size_t num_nodes;

	if (argc != 5  || (strcmp(argv[1], "-ip") != 0) || (strcmp(argv[3], "-nodes") != 0)) {
	    fprintf(stderr,"usage: server -ip <host-ip-address>:PORT -nodes <num_nodes>\n");
	    _exit(1);
	} 
	ip = strtok(argv[2], ":");
    port = std::stoi(strtok(NULL, ":"));
    //std::cout << "port:"<< port << "\n";
	num_nodes = std::stoi(argv[4]);
    //std::cout << "nodes:"<< num_nodes << "\n";

    NetworkIP* client_ = new NetworkIP();

    client_->server_init(0, ip, port, num_nodes);
    return 0;
}
