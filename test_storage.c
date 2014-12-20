#include <stdio.h>
#include <fcntl.h>

#include "bufmgr.h"

void test_readBuffer()
{
	NBuffers = 3;
	NBufTables = 1;

	initBuffer();
	int n = 4;
	int i, j;
	for (j = 0; j < 2; j++)
	for (i = 0; i < n; i++) {
		BufferTag buf_tag = {{0,0},i};
		int buf_id = readBuffer(buf_tag, 0);
		void *buf_block = GetBufblockFromId(buf_id);
		printf("buf_id = %d\n", buf_id);
		if (i == 0) {
			strcpy(buf_block, "aaaaaaaaaaaaa");
			buffer_descriptors[i].buf_flag |= BUF_DIRTY;
		}
		int fd;
		fd = open("/home/wcl/Course/DBMS/01/data/0/out",O_RDWR|O_APPEND);
		write(fd, (char *)buf_block, PAGE_SIZE);
		char * test = "\n\n----------123456789\n\n";
		write(fd, (char *)test, 23);
		close(fd);
	}
}

/*int main(int argc, char* argv[]){
	test_readBuffer();

	return 0;
}*/
