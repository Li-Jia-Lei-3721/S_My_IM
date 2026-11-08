cmake_minimum_required(VERSION 3.10)
project(S_My_IM)

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

include_directories(${PROJECT_SOURCE_DIR}/third_party)

include_directories(/usr/include/hiredis)

# 生成服务端程序
add_executable(im_server 
				src/main.cpp
				src/network/tcp_server.cpp
				src/network/epoll_server.cpp
				src/db/mysql_pool.cpp
				src/db/redis_client.cpp 
				src/common/thread_pool.cpp
)

link_directories(/usr/lib64/mysql)

target_link_libraries(im_server 
	/usr/lib64/mysql/libmysqlclient.so 
	hiredis
    pthread
)



# 生成客户端程序
add_executable(im_client 
    			client/main.cpp)

