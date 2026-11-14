cmake_minimum_required(VERSION 3.10)
project(S_My_IM)

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

include_directories(${PROJECT_SOURCE_DIR}/third_party)

include_directories(/usr/include/hiredis)

# 生成服务端程序
add_executable(im_server 
				src/main.cpp
				src/network/epoll_server.cpp
				src/db/mysql_pool.cpp
				src/db/redis_client.cpp 
				src/common/thread_pool.cpp
				src/common/password_util.cpp
				src/common/session_manager.cpp
				src/common/logger.cpp
				src/common/validator.cpp
				src/common/config_manager.cpp 
)

target_link_libraries(im_server 
	/usr/lib64/mysql/libmysqlclient.so 
	hiredis
    pthread
	crypto			# OpenSSL加密库
	ssl 			# OpenSSL 安全传输库
)

add_definitions(-DPROJECT_ROOT="${CMAKE_SOURCE_DIR}")

# 生成客户端程序
add_executable(im_client 
    			client/main.cpp)

# 把配置文件复制到可执行文件所在目录
configure_file(
    ${PROJECT_SOURCE_DIR}/config.json
    ${EXECUTABLE_OUTPUT_PATH}/config.json
    COPYONLY
)



