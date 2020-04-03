// code adapted from: https://www.geeksforgeeks.org/socket-programming-in-cc-handling-multiple-clients-on-server-without-multi-threading/
#include <stdio.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <unistd.h> 
#include <string.h> 
#include "string.h"
#include "client.h"
#include <iostream>
#define TRUE 1 
#define PORT 8888
#define SERVER_IP "127.0.0.1"

//authors: shetty.y@husky.neu.edu, eldrid.s@husky.neu.edu

int main(int argc, char *argv[]) 
{ 
	int opt = TRUE;
	int sock = 0, valread; 
	struct sockaddr_in serv_addr; 
	char buffer[1024] = {0}; 

	if (argc != 3  || (strcmp(argv[1], "-ip"))) {
	    fprintf(stderr,"usage: server -ip <host-ip-address>:PORT\n");
	    _exit(1);
	} 
	char * ip = strtok(argv[2], ":");
    char * port = strtok(NULL, ":");

	Client* client = new Client(ip, port);
	
	StrBuff* address_buf = new StrBuff();
	//build the string to send
	address_buf->c(ip);
	address_buf->c(":");
	address_buf->c(port);

	char* ip_str = address_buf->get()->c_str();	
	std::cout << ip_str <<"\n";
	
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(PORT); 
	
	// Convert IPv4 and IPv6 addresses from text to binary form 
	if(inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr)<=0) 
	{ 
		printf("\nInvalid address/ Address not supported \n"); 
		return -1; 
	}

	client->connect_to_server(serv_addr);
	client->init_listening_socket(ip, port);
	client->sock_listen();
	client->register_with_server();
    client->start();  
	return 0; 
} 
