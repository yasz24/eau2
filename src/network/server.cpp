// code adapted from: https://beej.us/guide/bgnet/html
#include "string.h"
#include "server.h"

//authors: shetty.y@husky.neu.edu, eldrid.s@husky.neu.edu

#define PORT "8888"   // port we're listening on

int main(int argc , char *argv[]) {
    if (argc != 3  || (strcmp(argv[1], "-ip"))) {
	    fprintf(stderr,"usage: server -ip <host-ip-address>:PORT\n");
	    exit(1);
	}

    char * ip = strtok(argv[2], ":");
    char * port = strtok(NULL, ":");

    Server* server = new Server();
    server->init_socket(ip, port);
    server->sock_listen();
    server->start();
    return 0;
}
