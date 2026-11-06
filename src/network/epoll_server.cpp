#include "epoll_server.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>
//#include <algorithm>
#include <cerrno>
#include "../../third_party/json.hpp"
using json = nlohmann::json;

EpollServer::EpollServer(int port) : port_(port), server_fd_(-1), epoll_fd_(-1) {}

EpollServer::~EpollServer() {
    if (server_fd_ != -1)
        close(server_fd_);
    if (epoll_fd_ != -1)
        close(epoll_fd_);
}

void EpollServer::closeConnection(int client_fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    close(client_fd);
    clients_.erase(client_fd);
}

void EpollServer::setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool EpollServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;

    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port=htons(port_);

    if (bind(server_fd_, (struct sockaddr *)&address, sizeof(address)) == -1)
        return false;
    if (listen(server_fd_, 10) == -1)
        return false;

    setNonBlocking(server_fd_);

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == -1)
        return false;

    struct epoll_event ev;
    ev.events = EPOLLIN|EPOLLET;
    ev.data.fd = server_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev);

    cout << "Epoll Server started on port " << port_ << endl;
    return true;
}

void EpollServer::run() {
    struct epoll_event events[1024];//存放就绪事件

    while (true) {
        int nfds = epoll_wait(epoll_fd_, events, 1024, -1);
        if (nfds == -1)
            continue;

        for (int i = 0; i < nfds; i++) {
            int current_fd = events[i].data.fd;
            uint32_t evs = events[i].events;
            
            if (current_fd == server_fd_) {
                handleNewConnection();
                continue;
            }

            if (evs & EPOLLRDHUP || evs & EPOLLHUP || evs&EPOLLERR) {
                cout << "Client " << current_fd << " half-close/error." << endl;
                closeConnection(current_fd);
                continue;
            }

            if (evs & EPOLLIN) 
                handleClientData(current_fd);

            if (evs & EPOLLOUT)
                handleWrite(current_fd);
		}
	}
}

void EpollServer::handleNewConnection() {
    struct sockaddr_in client_addr;

    while (true) {
        socklen_t len = sizeof(client_addr);
        
        int client_fd=accept(server_fd_,(struct sockaddr*)&client_addr,&len);
        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)//连接拿光了
                break;
            else{
                perror("accept error");//其他错误如文件描述符耗尽等
                break;
            }
        }
        setNonBlocking(client_fd);
        //cout << "New client connected! fd = " << client_fd << endl;

        clients_.emplace(client_fd, ClientContext{client_fd, "",""});
        
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
	}
}

void EpollServer::handleClientData(int client_fd) {
    char buffer[1024];
    
    while (true) {
        memset(buffer,0,sizeof(buffer));

        //非阻塞模式下 没有数据revc会返回-1  并把errno设为EAGAIN
        int bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read > 0) {
            //cout << "Client " << client_fd << " say: " << buffer << endl;
			clients_[client_fd].recv_buffer.append(buffer,bytes_read);
        }
        else if (bytes_read == 0) {//客户端断开连接
            //cout << "Client " << client_fd << " disconnected." << endl;
            closeConnection(client_fd);
            break;
        }
        else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            else if(errno==EINTR){//信号被中断
				continue;
            }
            else {
              	closeConnection(client_fd);
                return;
            }
		}
    }

    // 从缓冲区切出消息
    while (true) {
        auto it = clients_.find(client_fd);
        if (it == clients_.end())
            return;

        string &rb = it->second.recv_buffer;

        if (rb.size() < sizeof(MessageHeader))
            break;

        MessageHeader header;
        memcpy(&header, rb.data(), sizeof(MessageHeader));

        if (header.magic != 0x12345678) {
            cerr << "Client " << client_fd << " sent invaild magic number! Disconnecting." << endl;
            closeConnection(client_fd);
            return;
		}

        if (header.length > 10 * 1024 * 1024) {
            cerr << "Client " << client_fd << " sent too large packet! Disconnecting." << endl;
            return;
		}
                
        if (rb.size() < sizeof(MessageHeader) + header.length)
            break;

        string body = rb.substr(sizeof(MessageHeader), header.length);
        rb.erase(0, sizeof(MessageHeader) + header.length);

        if (header.type == MSG_REGISTER || header.type == MSG_LOGIN) {
            try {
                // 1. 解析 JSON
                json j = json::parse(body);
                string uname = j["username"];
                string pwd = j["password"];

                // 2. 获取数据库连接（RAII机制，离开作用域自动归还）
                MYSQL *conn = nullptr;
                MysqlConnRAII raii(&conn);

                if (!conn) {
                    json res;
                    res["message"] = "数据库系统繁忙 请稍后再试！！！";
                    sendToClient(client_fd, MSG_SYSTEM, res.dump());
                    continue;
				}

				// 3. 防 SQL 注入转义（提取到公共部分）
                char escape_uname[256] = {0};
                char escape_pwd[256] = {0};

				mysql_real_escape_string(conn,escape_uname,uname.c_str(),uname.length());
                mysql_real_escape_string(conn, escape_pwd, pwd.c_str(), pwd.length());
                
                if (header.type == MSG_REGISTER) {
                    string sql = "INSERT INTO users (username,password) VALUES ('" + string(escape_uname) + "','" +
                                 string(escape_pwd) + "')";
                    if (mysql_query(conn, sql.c_str()) == 0) {
                        json res;
                        res["message"] = "注册成功 请登录";
                        sendToClient(client_fd,MSG_SYSTEM,res.dump());
                    }
                    else {
                        if (mysql_errno(conn) == 1062) {
                            json res; res["message"] = "注册失败，用户名已存在";
                            sendToClient(client_fd, MSG_SYSTEM, res.dump());
                        }
                        else {
							json res; res["message"] = "注册失败，服务器内部错误";
                            sendToClient(client_fd, MSG_SYSTEM, res.dump());
						}
					}
                }
                else if (header.type == MSG_LOGIN) {
                    string sql = "SELECT id FROM users WHERE username='" + string(escape_uname) + "' AND password='" +
                                 string(escape_pwd) + "'";
                    if (mysql_query(conn, sql.c_str()) == 0) {
                        MYSQL_RES *result = mysql_store_result(conn);
                        if (result) {
                            if (mysql_num_rows(result) > 0) {
                                clients_[client_fd].is_logged_in = true;
                                clients_[client_fd].username = uname;

                                json res; res["message"] = "登录成功，欢迎 " + uname;
                                sendToClient(client_fd, MSG_SYSTEM, res.dump());
                            }
                            else {
                                json res;
                                res["message"] = "登录失败，账号或密码错误";
                                sendToClient(client_fd, MSG_SYSTEM, res.dump());
                            }
                            mysql_free_result(result);
                        }
                        else {
                            json res; res["message"] = "登录失败，获取数据异常";
    						sendToClient(client_fd, MSG_SYSTEM, res.dump());
    						continue;
						}
                    }
                    else {
                        json res;
                        res["message"] = "登录失败，数据库查询错误";
                        sendToClient(client_fd, MSG_SYSTEM, res.dump());
                        continue;
					}
				}
            }
            catch (const exception &e) {
                json res;
                res["message"] = "数据格式错误";
                sendToClient(client_fd, MSG_SYSTEM, res.dump());
                break;                          
			}
        }
        else if (header.type == MSG_CHAT) {
            if (!clients_[client_fd].is_logged_in) {
                sendToClient(client_fd,MSG_SYSTEM,"请先登录!");
            }
            else {
                try {
                    json j = json::parse(body);
                    string input_msg = j["message"];

                    json out_json;
                    out_json["sender"] = clients_[client_fd].username;
                    out_json["message"] = input_msg;
                    string out_body = out_json.dump();

                    MessageHeader out_header;
                    out_header.magic = 0x12345678;
                    out_header.length = out_body.length();
                    out_header.type = MSG_CHAT;

                    string out_packet;
                    out_packet.append((char *)&out_header, sizeof(MessageHeader));
                    out_packet.append(out_body);

                    broadcastMessage(client_fd, out_packet.data(), out_packet.size());
                }
                catch (const exception &e) {
                    json res;
                	res["message"] = "数据格式错误";
                	sendToClient(client_fd, MSG_SYSTEM, res.dump());
                	break; 
				}
			}
        }
        if (clients_.find(client_fd) == clients_.end())
        return;
    }
}

void EpollServer::sendToClient(int client_fd, MsgType type, const string& body) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end())
        return;

    MessageHeader header;
    header.magic = 0x12345678;
    header.length = body.size();
    header.type = type;

    auto &ctx = it->second;
    ctx.send_buffer.append((char *)&header, sizeof(MessageHeader));
    ctx.send_buffer.append(body);

    while (!ctx.send_buffer.empty()) {
        int bytes_sent = send(client_fd, ctx.send_buffer.data(), ctx.send_buffer.size(), MSG_NOSIGNAL);
        if (bytes_sent > 0) {
            ctx.send_buffer.erase(0,bytes_sent);
        }
        else if (bytes_sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
                ev.data.fd = client_fd;
                epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
                break;
            }
            else {
                closeConnection(client_fd);
                break;
			}
        }
	}
}

void EpollServer::broadcastMessage(int sender_fd,const char* msg,int len) {//丢失数据版本
	vector<int> dead_fds;//存放抛弃的客户端
    
    for (auto &pair : clients_) {
        int fd = pair.first;
        if (fd == sender_fd)
            continue;

        auto &ctx = pair.second;

        // 积压太多直接删掉防止内存被撑爆
        if (ctx.send_buffer.size() > 10 * 1024 * 1024) {
            dead_fds.emplace_back(fd);
            continue;
		}
        
        ctx.send_buffer.append(msg, len);

        while (!ctx.send_buffer.empty()) {
            int bytes_sent = send(fd, ctx.send_buffer.data(), ctx.send_buffer.size(), MSG_NOSIGNAL);

            if (bytes_sent > 0) {
                ctx.send_buffer.erase(0,bytes_sent);
            }
            else if (bytes_sent == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET | EPOLLOUT;

                    ev.data.fd = fd;
                    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);

                    break;
                } else { // 其它错误 断开连接
                    dead_fds.emplace_back(fd);
					break;
				}
			}
		}
    }

    for (int dead_fd : dead_fds)
        closeConnection(dead_fd);
}

void EpollServer::handleWrite(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end())
        return;

    auto &ctx = it->second;
    while (!ctx.send_buffer.empty()) {
        int bytes_sent = send(client_fd, ctx.send_buffer.data(), ctx.send_buffer.size(), MSG_NOSIGNAL);
        if (bytes_sent > 0) 
            ctx.send_buffer.erase(0, bytes_sent);
        else {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            else {
                closeConnection(client_fd);
				return;
            }
		}
    }

    if (ctx.send_buffer.empty()) {
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
	}
}
