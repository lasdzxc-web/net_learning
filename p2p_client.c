#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/time.h>

#define REGISTER_FLAG_DATA     1
#define REGISTER_FLAG_ACCEPT   2
#define REGISTER_FLAG_CONNECT  3 

#define SETEVENT_FLAG_ADD      1
#define SETEVENT_FLAG_MOD      2


#define INITCLIENT_FLAG_ACCEPT 1
#define INITCLIENT_FLAG_CONNECT 2



#define PORT_ACCEPT   2000
#define BUFFER_LENGTH 1024
#define CONN_SIZE    1024*1024
#define MAX_PORT      1
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

typedef int (*RCALLBACK)(int fd);

struct conn
{
    int fd;
    
    int status;

    char rbuffer[BUFFER_LENGTH];
    int rlength;

    char wbuffer[BUFFER_LENGTH];
    int wlength;
    
    union{
    RCALLBACK send_callback;
    RCALLBACK connect_callback;
    }s_action;
//union一段内存，两个名字
    union{
    RCALLBACK recv_callback;
    RCALLBACK accept_callback;
    }r_action;
};


int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);

int epfd = 0;



struct conn conn_list[CONN_SIZE] = {0};


static int ntySetNonblock(int fd) {
	int flags;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0) return flags;
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0) return -1;
	return 0;
}

static int ntySetReUseAddr(int fd) {
	int reuse = 1;
	return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(reuse));
}

//初始化客户端，对指定端口建立监听套接字，为后续连接做准备,ip地址输入为空为被动连接，IP地址输入不为空则为主动连接
int init_client(const int port ,const char *ip){
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(ip == NULL || ip[0] == '\0'){

        struct sockaddr_in clientaddr;
        clientaddr.sin_family = AF_INET;
        clientaddr.sin_addr.s_addr = htonl(INADDR_ANY); // 0.0.0.0
        clientaddr.sin_port = htons(port); // 0-1023, 

        if (-1 == bind(sockfd, (struct sockaddr*)&clientaddr, sizeof(struct sockaddr))) {
            printf("bind failed: %s\n", strerror(errno));
        }

        listen(sockfd, 10);
        printf("listen finshed: %d\n", sockfd); // 3 
    }else{
        
        struct sockaddr_in clientaddr;
        clientaddr.sin_family = AF_INET;
        clientaddr.sin_addr.s_addr = inet_addr(ip); // 
        clientaddr.sin_port = htons(port); // 
        if (connect(sockfd, (struct sockaddr*)&clientaddr, sizeof(struct sockaddr_in)) < 0) {
				perror("connect");
    }
    }
    return sockfd;
}



//连接标识符属性设置，可以实现监听和监听事件的修改
int set_event(int fd, int event,int flag){
    if(fd < 0 || fd >= CONN_SIZE){
        return -1;
    }
//flag用来区别设置监听还是修改监听事件
    if(flag == SETEVENT_FLAG_ADD){
        struct epoll_event ev;

	    ev.events = event;
	    ev.data.fd = fd;
	    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
        }else if(flag == SETEVENT_FLAG_MOD){
        struct epoll_event ev;
	    ev.events = event;
	    ev.data.fd = fd;
	    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        }
}

//注册，实现fd的回调函数，缓存空间的初始化,tag=1为传输数据，tag=0为尝试建立连接
int event_register(int fd, int event,int tag){
    if(fd < 0 || fd >= CONN_SIZE){
        return -1;
    }
    conn_list[fd].fd = fd;
    if(tag == REGISTER_FLAG_DATA){
        conn_list[fd].r_action.recv_callback = recv_cb;
        conn_list[fd].s_action.send_callback = send_cb;
    }else if(tag == REGISTER_FLAG_ACCEPT){
        conn_list[fd].r_action.accept_callback = accept_cb;
    }
    
    

    memset(conn_list[fd].rbuffer, 0, BUFFER_LENGTH);
    conn_list[fd].rlength = 0;
 
    memset(conn_list[fd].wbuffer, 0, BUFFER_LENGTH);
    conn_list[fd].wlength = 0;

    set_event(fd, event,SETEVENT_FLAG_ADD);
}




//建立新连接，在监听套接字监听到客户端发起连接
int accept_cb(int fd){
    if(fd < 0 || fd >= CONN_SIZE){
        return -1;
    }
    struct sockaddr_in  clientaddr;
	socklen_t len = sizeof(clientaddr);

    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    if(clientfd < 0) {
        return -1;
    }
    event_register(clientfd, EPOLLIN,REGISTER_FLAG_DATA);

    return 0;

}

//接收数据，并将关注事件改为EPOLLOUT
int recv_cb(int fd){

    if(fd < 0 || fd >= CONN_SIZE){
        return -1;
    }
//将数据接收到rbuffer
    memset(conn_list[fd].rbuffer, 0, BUFFER_LENGTH );
    int count = recv(fd, conn_list[fd].rbuffer, BUFFER_LENGTH, 0);
    conn_list[fd].rlength = count; 
//disconnect
	if (count == 0) { 
		printf("client disconnect: %d\n", fd);
		close(fd);

		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
					
		return 0;
		}

        printf("RECV: %s\n", conn_list[fd].rbuffer);
//将数据从rbuffer copy 到 wbuffer
        conn_list[fd].wlength = conn_list[fd].rlength;
        

//更换关注事件
        set_event(fd, EPOLLIN,SETEVENT_FLAG_MOD);
        return count;
}

//将数据发送，并将fd关注事件调整为EPOLLIN
int send_cb(int fd){
    if(fd < 0 || fd >= CONN_SIZE){
        return -1;
    }
    conn_list[fd].wlength = sprintf(conn_list[fd].wbuffer, "Hello ,kair0see");
    int count = send(fd,conn_list[fd].wbuffer,conn_list[fd].wlength,0);
    
    set_event(fd, EPOLLIN,SETEVENT_FLAG_MOD);
    
    return count;
}






int main(int argc,char **argv){
    if (argc <= 2) {
		printf("Usage: %s ip port\n", argv[0]);
		exit(0);
	}
    const char *ip_connect = argv[1];
    unsigned short port_connect = atoi(argv[2]);

    unsigned short port_accept = PORT_ACCEPT;
//创建epoll
    epfd = epoll_create(1);

//开始监听等待连接
    int sockfd_accept = init_client(port_accept,NULL);
    event_register(sockfd_accept,EPOLLIN,REGISTER_FLAG_ACCEPT);

//主动发起连接,sockfd_connect没封装进函数有点可惜,但是sockfd_connect由于Init时候不用绑定，导致我得传地址参数进去，和回调函数通式不一样了
    int sockfd_connect = init_client(port_connect,ip_connect);
    event_register(sockfd_connect,EPOLLOUT,REGISTER_FLAG_DATA);

    //接下来就是发送数据，或者说循环发送数据，之后要对发送双方连接进行测试，就是在两边同时建立连接，且两边都能在接受数据的同时发送数据
    ntySetNonblock(sockfd_connect);
	ntySetReUseAddr(sockfd_connect);

//mainloop
    while (1){
//创建用于接受就绪事件的集合
        struct epoll_event events[1024]={0};
//等待就绪事件
        int nready=epoll_wait(epfd, events, 1024, -1);
//逐个处理就绪事件，reactor的特点，按就绪事件类型来处理
        int i = 0;
        for (i=0; i<nready; i++ ){
            int connfd = events[i].data.fd;

            if((events[i].events & EPOLLIN)){
                conn_list[connfd].r_action.recv_callback(connfd);
            }

            if(events[i].events & EPOLLOUT){
                conn_list[connfd].s_action.send_callback(connfd);
            }

        }


    }

}
