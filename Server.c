#include <stdio.h>
#include <stdlib.h>

#include <sys/socket.h>
#include <netinet/in.h>

#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) 
{
	int ar=0;
	if (argc != 3) 
	{
		printf("Usage: ./program -p <port>\n");
		return 0;
	} 

	//int port = atoi(argv[2]);
	int success;
	char buffer[2560];
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in serv_addr, cli_addr;

	serv_addr.sin_port = htons(atoi(argv[2]));
	serv_addr.sin_family = AF_INET;

	bind(sockfd, (struct sockaddr *)  &serv_addr, sizeof(serv_addr));
	listen(sockfd, 5);
	int clilen = sizeof(cli_addr);

	printf("LISTENING\n");

	int incomingsockfd;
	//accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);

	if (sockfd < 0) {
		printf("Error opening socket!\n");
		return 0;
	}

	printf("LISTENED\n");

	while((incomingsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen))>=0) {
		bzero(buffer,2560);
	    	success = read(incomingsockfd,buffer,2559);
	    	write(incomingsockfd, buffer, strlen(buffer));
		//sleep(1);
		if (success < 0) {
			printf("ERROR READING\n");
			return 0;
		}
		printf("Here is the message: %s\n",buffer);
	}
	return 0;
}
