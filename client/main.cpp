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
#include <atomic>

int client_sock;

atomic<bool> g_login_ok{false};

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

				// 转包头字节序
				header.unpack();

				if (header.magic != 0x12345678) {
					cerr << "协议错误，强行断开" << endl;
					close(client_sock);
					exit(1);
				}

				if (recv_buffer.size() < sizeof(MessageHeader) + header.length) break;

				string body = recv_buffer.substr(sizeof(MessageHeader), header.length);
				recv_buffer.erase(0, sizeof(MessageHeader) + header.length);

				try {
					json j = json::parse(body);
					if (header.type == MSG_CHAT) {
						if (j.contains("room")) {
							// 房间消息，显示 [roomname:username]message
							cout << "\n[" << j["room"].get<string>() << ":" << j["sender"].get<string>() << "]"
								 << j["message"].get<string>() << endl;
						}
						else {
							string sender = j["sender"];
							string msg = j["message"];
							cout << "\n[" << sender << "]: " << msg << endl;
						}
					}
					else if (header.type == MSG_SYSTEM) {
						string msg = j["message"];
						cout << "\n[系统]: " << msg << endl;
						if (msg.find("登录成功") != string::npos) {
							g_login_ok = true;
						}
					}
				}
				catch (const exception &e) {
					cerr << "解析JSON失败：" << e.what() << endl;
				}
				cout << "> " << flush;
			}
		}
		else if (bytes_read == 0) {
			cout << "服务器断开连接." << endl;
			exit(0);
		}
		else {
			cerr << "recv 发生错误 退出" << endl;
			exit(1);
		}
	}
}

void send_packet(MsgType type, const json &j) {
	string body = j.dump();
	MessageHeader header;
	header.magic = 0x12345678;
	header.length = body.size();
	header.type = type;
	header.pack(); // 转换为网络字节序

	string packet = string((char *)&header, sizeof(MessageHeader)) + body;
	send(client_sock, packet.data(), packet.size(), 0);
}

void chat_loop() {
	string input;
	while (true) {
		cout << "> " << flush;
		getline(cin, input);
		if (input == "exit" || input == "quit") {
			exit(0);
		}
		if (input.empty()) continue;
		json chat_json;

		if (input.rfind("/join", 0) == 0) {
			string room_name = input.substr(6);
			if (room_name.empty()) continue;
			chat_json["room_name"] = room_name;
			send_packet(MSG_JOIN_ROOM, chat_json);
		}
		else if (input[0] == '#') {
			size_t sp = input.find(' ');
			if (sp == string::npos) continue;
			chat_json["room_name"] = input.substr(1, sp - 1);
			chat_json["message"] = input.substr(sp + 1);
			send_packet(MSG_ROOM_CHAT, chat_json);
		}
		else if (input[0] == '@') {
			size_t space_pos = input.find(' ');
			if (space_pos != string::npos) {
				chat_json["target"] = input.substr(1, space_pos - 1);
				chat_json["message"] = input.substr(space_pos + 1);
				send_packet(MSG_CHAT, chat_json);
			}
		}
		else {
			chat_json["message"] = input;
			send_packet(MSG_CHAT, chat_json);
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

	while (true) {
		cout << "1.注册 2.登录";
		cout << "\n进入聊天模式。\n"
			 << "  普通消息         = 群聊\n"
			 << "  @用户名 消息     = 私聊\n"
			 << "  /join 房间名     = 加入/创建房间\n"
			 << "  #房间名 消息     = 在房间发消息\n"
			 << "  exit             = 退出\n";
		int choice;

		if (!(cin >> choice)) {
			cin.clear();
			cin.ignore(10000, '\n');
			continue;
		}

		cin.ignore();

		if (choice == 3) {
			cout << "再见" << endl;
			return 0;
		}

		string uname, pwd;
		cout << "请输入用户名: ";
		getline(cin, uname);
		cout << "请输入密码: ";
		getline(cin, pwd);

		if (choice == 1) {
			// ---------- 注册流程 ----------
			// 1. 请求验证码
			json req_code;
			req_code["username"] = uname;
			send_packet(MSG_GET_CODE, req_code);

			// 2. 服务端会通过 MSG_SYSTEM 把验证码推过来（recv 线程打印）
			// 稍等一下让日志刷出来
			this_thread::sleep_for(chrono::milliseconds(300));

			cout << "请输入收到的验证码: ";
			string code;
			getline(cin, code);

			// 3. 发送注册请求
			json reg_json;
			reg_json["username"] = uname;
			reg_json["password"] = pwd;
			reg_json["code"] = code;
			send_packet(MSG_REGISTER, reg_json);

			cout << "注册请求已发送，等待服务器响应..." << endl;
			// 回到菜单，用户可继续选登录
			continue;
		}

		if (choice == 2) {
			// ---------- 登录流程 ----------
			json login_json;
			login_json["username"] = uname;
			login_json["password"] = pwd;
			g_login_ok = false;
			send_packet(MSG_LOGIN, login_json);

			// 最多等 3 秒登录结果
			for (int i = 0; i < 30 && !g_login_ok; i++) {
				this_thread::sleep_for(chrono::milliseconds(100));
			}
			if (!g_login_ok) {
				cout << "登录失败或超时，请重试" << endl;
				continue;
			}
			g_login_ok = false;

			// 登录成功，进入聊天
			cout << "\n进入聊天模式。普通消息=群聊，@用户名 消息=私聊,exit退出\n";
			chat_loop();
		}
	}
	close(client_sock);
	return 0;
}
