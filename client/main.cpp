#include "../src/protocol/message.h"
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
using namespace std;
#include "../third_party/json.hpp"
using json = nlohmann::json;

int client_sock;

void receive_thread() {
	char buffer[1024];
	string recv_buffer;

	while (true) {
		memset(buffer, 0, sizeof(buffer));
		int bytes_read = recv(client_sock, buffer, sizeof(buffer), 0);

		if (bytes_read > 0) {
			recv_buffer.append(buffer, bytes_read);

			while (recv_buffer.size() >= sizeof(MessageHeader)) {
				MessageHeader header;
				memcpy(&header, recv_buffer.data(), sizeof(MessageHeader));

				if (header.magic != 0x12345678) {
					cerr << "协议错误，强行断开" << endl;
					close(client_sock);
					exit(1);
				}

				if (recv_buffer.size() < sizeof(MessageHeader) + header.length)
					break;

				string body = recv_buffer.substr(sizeof(MessageHeader), header.length);
				recv_buffer.erase(0, sizeof(MessageHeader) + header.length);

				try {
					json j = json::parse(body);
					if (header.type == MSG_CHAT) {
						string sender = j["sender"];
						string msg = j["message"];
						cout << "\n[" << sender << "]: " << msg << endl;
					} else if (header.type == MSG_SYSTEM) {
						cout << "\n[系统]: " << j["message"] << endl;
					}
				} catch (const exception &e) {
					cerr << "解析JSON失败：" << e.what() << endl;
				}
				cout << "> " << flush;
			}
		} else if (bytes_read == 0) {
			cout << "服务器断开连接." << endl;
			exit(0);
		} else {
			cerr << "recv 发生错误 退出" << endl;
			exit(1);
		}
	}
}

int main() {
	client_sock = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	server_addr.sin_port = htons(8080);

	if (connect(client_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
		cerr << "连接服务器失败！请确认服务器已开启." << endl;
		return -1;
	}

	cout << "连接成功" << endl;

	thread recv_t(receive_thread);
	recv_t.detach();

	cout << "请输入 1: 注册  2. 登录" << endl;
	int choice;
	cin >> choice;
	cin.ignore();

	string uname, pwd;
	cout << "请输入用户名: ";
	getline(cin, uname);
	cout << "请输入密码: ";
	getline(cin, pwd);

	string auth_body;
	json auth_json;
	auth_json["username"] = uname;
	auth_json["password"] = pwd;
	auth_body = auth_json.dump();

	MessageHeader auth_header;
	auth_header.magic = 0x12345678;
	auth_header.length = auth_body.size();
	auth_header.type = (choice == 1) ? MSG_REGISTER : MSG_LOGIN;

	string auth_packet;
	auth_packet.append((char *)&auth_header, sizeof(MessageHeader));
	auth_packet.append(auth_body);
	send(client_sock, auth_packet.data(), auth_packet.size(), 0);

	string input;
	while (true) {
		cout << ">" << flush;
		getline(cin, input);

		if (input == "exit" || input == "quit") {
			exit(0);
		}

		if (input.empty())
			continue;

		string chat_body;
		json chat_json;
		if (input[0] == '@') {
			size_t space_pos = input.find(' ');
			if (space_pos != string::npos) {
				chat_json["target"] = input.substr(1, space_pos - 1);
				chat_json["message"] = input.substr(space_pos + 1);
			} else {
				continue;
			}
		} else {
			chat_json["message"] = input;
		}
		chat_body = chat_json.dump();
		MessageHeader header;
		header.magic = 0x12345678;
		header.length = chat_body.size();
		header.type = MSG_CHAT;

		string packet;
		packet.append((char *)&header, sizeof(MessageHeader));
		packet.append(chat_body);

		send(client_sock, packet.data(), packet.size(), 0);
	}

	close(client_sock);
	return 0;
}