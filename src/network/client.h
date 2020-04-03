// code adapted from: https://www.geeksforgeeks.org/socket-programming-in-cc-handling-multiple-clients-on-server-without-multi-threading/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "string.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "object.h"
#include <iostream>
#define TRUE 1 
#define PORT 8888
#define SERVER_IP "127.0.0.1"

//todo: don't need to take ip, port in constructor and the init_sock method both.
// nodes that connect to you and nodes that you initiate connection to are both clients.
class Client : public Object {
public:
    char* ip_addr;
    char* ip_port;
    int fdmax;
    size_t* client_fds; //owned
    char** client_addresses; //owned
    size_t num_clients;
    size_t max_clients; // max no.of clients you could have.
    fd_set master;    // master file descriptor list
    fd_set read_fds;  // temp file descriptor list for select()
    int yes=1;        // for setsockopt() SO_REUSEADDR, below
	struct sockaddr_in address;
	int server_sock = 0, valread;
    bool _listening_socket_initialized; 
	struct sockaddr_in serv_addr;
    char* ip_str;
    int listener;     // listening socket descriptor 
	char buffer[1024] = {0};

    Client(char * ip, char * port) {
        this->ip_addr = ip;
        this->ip_port = port;
        this->max_clients = 20; //because 
        this->num_clients = 0;
        this->client_addresses = new char*[max_clients];
        this->client_fds = new size_t[max_clients];
        //create a socket to communicate with the server.
        if ((this->server_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) { 
            printf("\n Socket creation error \n"); 
            exit(1);
	    }
        if( setsockopt(this->server_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) < 0 ) { 
            perror("setsockopt"); 
            exit(1); 
        }
        FD_SET(this->server_sock, &master);
        StrBuff* address_buf = new StrBuff();
        //build the string to send
        address_buf->c(ip);
        address_buf->c(":");
        address_buf->c(port);
        ip_str = address_buf->get()->c_str();	 
        //std::cout << ip_str << "\n";	
        delete address_buf; 
    }


    /**
     * ip: The ip address that you want to bind the socket to.
     * port: The port on which the socket should listen
    */
    void init_listening_socket(char* ip, char* port) {
        struct addrinfo hints, *ai, *p;
        int rv;

        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;

        if ((rv = getaddrinfo(ip, port, &hints, &ai)) != 0) {
            //invalid address.
            fprintf(stderr, "selectserver: %s\n", gai_strerror(rv));
            exit(1);
        }

        // get us a socket and bind it
        for(p = ai; p != NULL; p = p->ai_next) {
            this->listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (listener < 0) { 
                continue;
            }
            // lose the pesky "address already in use" error message
            setsockopt(this->listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
            
            if (bind(this->listener, p->ai_addr, p->ai_addrlen) < 0) {
                close(this->listener);
                continue;
            } else
            {
                struct sockaddr_in sin;
                socklen_t len = sizeof(sin);
                if (getsockname(this->listener, (struct sockaddr *)&sin, &len) == -1)
                    perror("getsockname");
                else
                    printf("client listener bound to port number %d\n", ntohs(sin.sin_port));
            }
            break;
        }

        if (p == NULL) {
            // if we got here, it means we didn't get bound
            fprintf(stderr, "selectserver: failed to bind\n");
            exit(1);
        }

        freeaddrinfo(ai); // all done with this
        this->_listening_socket_initialized = true; //server sock has been initialized.
    }

    void sock_listen() {
        if (!this->_listening_socket_initialized) {
            fprintf(stderr, "selectserver: unitialized socket\n");
            exit(1);
        }
        // listen
        if (listen(this->listener, max_clients) == -1) {
            perror("listen");
            exit(1);
        }
        // add the listener to the master set
        FD_SET(listener, &master);
    }

    int connect_to_server(struct sockaddr_in server_address) {
        this->serv_addr = server_address;
        if (connect(this->server_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) { 
		    printf("\nConnection Failed \n"); 
		    return -1; 
	    }
        return 0;
    }

    void register_with_server() {
        send(this->server_sock , this->ip_str , strlen(this->ip_str) , 0 );
        printf("Client %s:  Hello message sent\n", this->ip_str);
    }

    int receive_from_server(char* buffer, size_t buf_size, int flags) {
        int valread = recv(this->server_sock, buffer, buf_size, flags);
        // std::cout << "client read in value\n";
		printf("Client %s, received: \n", ip_str);
		printf("%s\n",buffer );
        return valread;
    }

    int start() {
        // keep track of the biggest file descriptor
        this->fdmax = this->listener; // so far, it's this one
        int newfd;        // newly accept()ed socket descriptor
        struct sockaddr_in remoteaddr; // client address
        socklen_t addrlen;
        char buf[256];    // buffer for client data
        int nbytes;

        // main loop
        while(this->num_clients < this->max_clients) {
            read_fds = master; // copy it
            this->_wait_activity(this->fdmax+1, &this->read_fds, NULL, NULL, NULL);

            // run through the existing connections looking for data to read
            for(size_t i = 0; i <= this->fdmax; i++) {
                if (FD_ISSET(i, &read_fds)) { // we got one!!
                    if (i == this->server_sock) {
                        // std::cout << "new activity server sock\n";
                        if (nbytes = recv(i, buf, sizeof buf, 0) <= 0) {
                            // server shutdown
                            exit(1); // think about this
                        } else {
                            //parse the client list and setup node connections
                            _handle_node_connection(buf);
                        }
                    }
                    else if (i == listener) {
                        // std::cout << "new activity listener sock\n";
                        // handle new connections
                        addrlen = sizeof remoteaddr;
                        newfd = accept(listener,
                            (struct sockaddr *)&remoteaddr,
                            &addrlen);
                        // std::cout << "handling client connection\n";
                        this->_handle_client_connection(newfd, &remoteaddr);
                    } else {
                        // std::cout << "msg from node\n";
                        // handle data from a client
                        if ((nbytes = recv(i, buf, sizeof buf, 0)) <= 0) {
                            this->_handle_client_shutdown(nbytes, i);
                        } else {
                            // std::cout << "i is:" << i << "\n";
                            // std::cout << "received buf: " << buf <<"\n";
                            //msg received from one of the other nodes, just log to console.
                            this->_handle_node_msg(buf, i);
                        }
                    } // END handle data from client
                } // END got new incoming connection
            } // END looping through file descriptors
        } // END while loop.
        return 0;   
    }

    void _handle_node_connection(char* buf) {
        //parse the client list and setup node connections
        std::cout << buf << "\n";
        char * list_ip = strtok(buf, "\n");
        //point to first ip
        list_ip = strtok(NULL, "\n");
        bool make_connections = false;
        while (list_ip != NULL) {
            if (make_connections) {
                //start making sockets and connecting to any clients that are not already in the list.
                if (!_in_ip_list(list_ip)) {
                    //if ip not in clients list of nodes then handle connections

                    char * ip = strtok(list_ip, ":"); //the client node's ip
                    char * port = strtok(NULL, ":"); //the client node's port
    
                    int connection_sock = socket(AF_INET, SOCK_STREAM, 0);

                    // Bind to a specific network interface (and optionally a specific local port)
                    struct sockaddr_in localaddr;
                    localaddr.sin_family = AF_INET;
                    localaddr.sin_addr.s_addr = inet_addr(this->ip_addr);
                    localaddr.sin_port = 0;  // Any local port will do
                    bind(connection_sock, (struct sockaddr *)&localaddr, sizeof(localaddr));
                    
                    //the client node's address
                    struct sockaddr_in node_addr;
                    node_addr.sin_family = AF_INET;
                    node_addr.sin_addr.s_addr = inet_addr(ip);
                    int node_port = std::stoi(port);
	                node_addr.sin_port = htons(node_port);   

                    if (connect(connection_sock, (struct sockaddr *)&node_addr, sizeof(node_addr)) == -1) {
                        close(connection_sock);
                        perror("client: connect");
                        list_ip = strtok(NULL, "\n");
                        continue; //go back to the top of while loop, process next ip.
                    }
                    //if we got here connection succeeded.
                    _handle_client_connection(connection_sock, (sockaddr_in *) &node_addr);
                    StrBuff* toSendBuf = new StrBuff();
                    toSendBuf->c("Hello from:");
                    toSendBuf->c(ip);
                    toSendBuf->c(":");
                    toSendBuf->c(node_port);
                    char* toSend = toSendBuf->get()->c_str(); 
                    delete toSendBuf;
                    //send hello message to ip:port
                    send(connection_sock, toSend, strlen(toSend), 0);
                }
            }
            // struct sockaddr_in sin;
            // socklen_t len = sizeof(sin);
            // if (getsockname(this->listener, (struct sockaddr *)&sin, &len) == -1) {
            //     perror("getsockname");
            // } else {
            //     printf("port number %d\n", ntohs(sin.sin_port));   
            // }
            if (strcmp(list_ip, this->ip_str) == 0) {
                make_connections = true;  
            }
            list_ip = strtok(NULL, "\n");
        }
    }

    bool _in_ip_list(char* node_ip) {
        for (size_t i = 0; i < this->num_clients; i++) {
            if (strcmp(this->client_addresses[i], node_ip) == 0) {
                return true;
            }
        }
        return false;
    }

    void _handle_client_connection(int newfd, sockaddr_in* remoteaddr) {
        socklen_t addrlen;
        // handle new connections
        addrlen = sizeof remoteaddr;

        if (newfd == -1) {
            perror("accept");
        } else {
            FD_SET(newfd, &master); // add to master set
            if (newfd > this->fdmax) {    // keep track of the max
                this->fdmax = newfd;
            }

            char* client_addr = inet_ntoa(remoteaddr->sin_addr);
            StrBuff* address_buf = new StrBuff();
            address_buf->c(client_addr);

            int client_port = ntohs(remoteaddr->sin_port);
            std::cout << "adding new client to list of connections: " << client_addr << ":" << client_port << "\n";
            //build the client port as a string.
            char portStr[20]; 
            snprintf(portStr, 20, "%d", client_port);
            address_buf->c(":");
            address_buf->c(portStr);

            for (size_t client = 0; client< max_clients; client++) {
                if (this->client_fds[client] == 0) {
                    this->client_fds[client] = newfd;
                    //add the incoming client connection's ip to the list of clients. 
                    this->client_addresses[client] = address_buf->get()->c_str();
                    this->num_clients++;
                    // printf("Adding to the list of sockets as fd %d\n", newfd);
                    break;
                }
            }
            delete address_buf;

            // printf("select_client_%s: new connection from %s on port %d on socket %d\n", this->ip_str,
            //     client_addr, client_port, newfd);
        }
    }

    void _handle_client_shutdown(int nbytes, int clientfd) {
        // got error or connection closed by client
        if (nbytes == 0) {
            // connection closed
            printf("select_client_%s: socket %d hung up\n", this->ip_str, clientfd);
        } else {
            perror("recv");
        }

        for (size_t l = 0; l < max_clients; l++) {
            if (this->client_fds[l] == clientfd) {
                std::cout << "shutting down client fd " << clientfd << "\n";
                this->client_fds[l] = 0; //reset the status.
                this->client_addresses[l] = ""; //set the address to empty str.
            } 
        }
        num_clients--;

        close(clientfd); // bye!
        FD_CLR(clientfd, &master); // remove from master set
    }

    int _wait_activity(int numfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout) {
        int select_val = select(numfds, readfds, writefds, exceptfds, timeout);
        if (select_val == -1) {
                perror("select failed");
                exit(4);
        }
        return select_val;
    }

    void _handle_node_msg(const char* buf, int clientfdd) {
        StrBuff* address_buf = new StrBuff();
        address_buf->c(buf);
        char * client_ip = "";
        char * node_msg = address_buf->get()->c_str();
        //find the ip of client that sent message.
        for (size_t i = 0; i < this->max_clients; i++) {
            if (this->client_fds[i] == clientfdd) {
                client_ip = this->client_addresses[i];
                break;
            }
        }
        printf("%s\n", node_msg);   
    }
};