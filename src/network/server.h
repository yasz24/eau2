#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "object.h"
#include "string.h"
#include <iostream>

//todo: rename listener to server sock.
/**
 * Server: A rendezvous server class, that supports multiple client registration.
 * Encapsulates the POSIX networking library functions and sockets that are used for connection management.
 * authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu 
*/
class Server : public Object {
public:
    fd_set master;    // master file descriptor list
    fd_set read_fds;  // temp file descriptor list for select()
    const char* PORT = "8888"; 
    size_t max_clients; // max no.of clients you could have.
    size_t* client_fds; //owned
    char** client_addresses; //owned
    size_t num_clients;
    bool _socket_initialized;
    int listener;     // listening socket descriptor
    int yes=1;        // for setsockopt() SO_REUSEADDR, below
    int fdmax;

    Server() {
        this->max_clients = 10;
        this->num_clients = 0;
        this->client_fds = new size_t[max_clients];
        this->_socket_initialized = false;
        //initialize to all zeros
        for (size_t k = 0; k < this->max_clients; k++) {
            this->client_fds[k] = 0;
        }
        FD_ZERO(&this->master);    // clear the master set
        FD_ZERO(&this->read_fds);  // clear the temp set
        this->client_addresses = new char*[max_clients];
    }

    /**
     * ip: The ip address that you want to bind the socket to.
     * port: The port on which the socket should listen
    */
    void init_socket(char* ip, char* port) {
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
            }
            break;
        }

        if (p == NULL) {
            // if we got here, it means we didn't get bound
            fprintf(stderr, "selectserver: failed to bind\n");
            exit(1);
        }

        freeaddrinfo(ai); // all done with this
        this->_socket_initialized = true; //server sock has been initialized.
    }

    void sock_listen() {
        if (!this->_socket_initialized) {
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
                    if (i == listener) {
                        // handle new connections
                        addrlen = sizeof remoteaddr;
                        newfd = accept(listener,
                            (struct sockaddr *)&remoteaddr,
                            &addrlen);
                        // std::cout << "handling client connection\n";
                        this->_handle_client_connection(newfd, remoteaddr);
                    } else {
                        // handle data from a client
                        if ((nbytes = recv(i, buf, sizeof buf, 0)) <= 0) {
                            this->_handle_client_shutdown(nbytes, i);
                        } else {
                            // std::cout << "i is:" << i << "\n";
                            // std::cout << "received buf: " << buf <<"\n";
                            //assume that the only type of message a client sends is registration.
                            this->_handle_registration(buf, i);
                        }
                    } // END handle data from client
                } // END got new incoming connection
            } // END looping through file descriptors
        } // END while loop.
        return 0;   
    }

    int _wait_activity(int numfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout) {
        int select_val = select(numfds, readfds, writefds, exceptfds, timeout);
        if (select_val == -1) {
                perror("select failed");
                exit(4);
        }
        return select_val;
    }

    void _handle_client_connection(int newfd, sockaddr_in remoteaddr) {
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
            // struct sockaddr_in sin;
            // socklen_t len = sizeof(sin);
            // if (getsockname(newfd, (struct sockaddr *)&sin, &len) == -1) {
            //     perror("getsockname");
            // } else {
            //     printf("port number %d\n", ntohs(sin.sin_port));   
            // }

            char* client_addr = inet_ntoa(remoteaddr.sin_addr);
            StrBuff* address_buf = new StrBuff();
            address_buf->c(client_addr);

            int client_port = ntohs(remoteaddr.sin_port);
            // std::cout << "client port: " << client_port << "\n";
            //build the client port as a string.
            char portStr[20]; 
            snprintf(portStr, 20, "%d", client_port);
            address_buf->c(":");
            address_buf->c(portStr);

            for (size_t client = 0; client< max_clients; client++) {
                if (this->client_fds[client] == 0) {
                    this->client_fds[client] = newfd;
                    //this->client_addresses[client] = address_buf->get()->c_str();
                    this->num_clients++;
                    //printf("Adding to the list of sockets as fd %d\n", newfd);
                    break;
                }
            }
            delete address_buf;

            printf("selectserver: new connection from %s on port %d on socket %d\n",
                client_addr, client_port, newfd);
        }
    }

    void _handle_client_shutdown(int nbytes, int clientfd) {
        // got error or connection closed by client
        if (nbytes == 0) {
            // connection closed
            printf("selectserver: socket %d hung up\n", clientfd);
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

    void _handle_registration(const char* buf, int clientfdd) {
        // std::cout << "adding address here\n";
        StrBuff* address_buf = new StrBuff();
        address_buf->c(buf);
        //assign the client_address at the right index.
        for (size_t i = 0; i < this->max_clients; i++) {
            if (this->client_fds[i] == clientfdd) {
                this->client_addresses[i] = address_buf->get()->c_str();
                break;
            }
        }
        std::cout << "handle client data\n";
        // we got some data from a client
        for(size_t j = 0; j <= this->fdmax; j++) {
            // send to everyone!
            if (FD_ISSET(j, &this->master)) {
                if (j != this->listener) {
                    // std::cout << "sending to client\n";
                    StrBuff* newStuff = new StrBuff();
                    newStuff->c("list:\n");
                    for (size_t client = 0; client < this->max_clients; client++) {
                        // except the listener and ourselves and if a client has connected
                        if (this->client_fds[client] != 0) {
                            newStuff->c(this->client_addresses[client]); 
                            newStuff->c("\n");

                        } 
                    }
                    char *toSend = newStuff->get()->c_str();
                    // std::cout << toSend;
                    if (send(j, toSend, strlen(toSend), 0) == -1) {
                                perror("send");
                    }
                    delete newStuff;
                }
            }
        }   
    }
};