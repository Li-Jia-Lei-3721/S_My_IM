#include "tcp_server.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
//#include <cstdlib>
#include <arpa/inet.h>

TcpServer::TcpServer(int port) : port_(port), server_fd_(-1) {}

TcpServer::~TcpServer() {
    if (server_fd_ != -1) {
        close(server_fd_);
	}
}

bool TcpServer::start() {
    server_fd_=socket(AF_INET,SOCK_STREAM,0);
    if (server_fd_ == -1) {
        perror("socket creation failed");
        return false;
	}

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    memset(&address,0,sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port=htons(port_);

    if (bind(server_fd_, (struct sockaddr *)&address, sizeof(address)) == -1) {
        perror("bind failed");
		return false;
    }

    if (listen(server_fd_, 10) == -1) {
		perror("listen failed");
        return false;
    }

    cout << "Server started on port " << port_ << endl;
    return true;
}

void TcpServer::acceptLoop() {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    while (true) {
        int client_fd = accept(server_fd_, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd == -1) {
            perror("accept failed");
            continue;
        }

        //cout<<"New client connected from IP:"<<inet_ntoa(client_addr.sin_addr)<<endl;
		cout<<"New client connected! fd = "<<client_fd<<endl;

        {
            lock_guard<mutex> lock(mtx_);
            client_fds_.emplace_back(client_fd);
		}
                
        thread client_thread(&TcpServer::handleClient, this, client_fd);
                
        client_thread.detach();        
	}
}

void TcpServer::handleClient(int client_fd) {
    char buffer[1024];

    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

        if (bytes_read > 0) {
            cout << "Client " << client_fd << " say " << buffer << endl;
            broadcastMessage(client_fd,buffer,bytes_read);
        } else if (bytes_read == 0) {
            cout << "Client " << client_fd << " disconnected. " << endl;
            break;
        } else {
            perror("recv error");
            break;
		}
    }
    close(client_fd);
    {
        lock_guard<mutex> lock(mtx_);
        client_fds_.erase(
            remove(client_fds_.begin(), client_fds_.end(), client_fd),
            client_fds_.end());
        
	}
}

void TcpServer::broadcastMessage(int sender_fd, const char *msg, int len) {
    lock_guard<mutex> lock(mtx_);

    for (int fd : client_fds_) {
        if (fd != sender_fd) {
            send(fd,msg,len,0);
		}
	}
}