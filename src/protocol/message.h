#pragma once
#include <cstdint>

struct __attribute__((packed)) MessageHeader {
    uint32_t magic;
    uint32_t length;
    uint16_t type;
};

enum MsgType {
    MSG_REGISTER = 1,	//注册
    MSG_LOGIN = 2,		//登录
    MSG_CHAT = 3,		//群聊
    MSG_SYSTEM = 4		//系统提示
};
