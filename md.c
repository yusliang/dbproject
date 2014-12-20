/*-------------------------------------------------------------------------
 *
 *	md.c
 *
 *	Manage Disk files
 *
 *	IDENTIFICATION
 *		src/md.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>

#include "storage.h"
#include "bufmgr.h"

extern char init_path[100];

/*
 *	Create a segment for a relation.
 *	Return 0 in case of success and -1 when failed.
 */
int mdCreate(RelationNode rel){
	int count;
	if((count = mdCount(rel)) < 0)							//get existing segment number of relation
		return -1;
	char file_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	if(constructPath(file_path, rel, count) < 0)
		return -1;
	if (DEBUG)
		printf("[INFO] mdCreate(): new file name is %s\n", file_path);
	int fd = open(file_path, O_CREAT | O_EXCL);				//create a segment
	if(fd < 0){
		if (DEBUG)
			printf("[INFO] mdCreate(): cannot create file %s\n", file_path);
		return -1;
	}
	close(fd);
	chmod(file_path, S_IRUSR | S_IWUSR);					//change access mode to be read & write
	char fsm_tbl[PAGE_SIZE];								//init first page to be freespace mapping table
	int i;
	for (i = 0; i < PAGE_SIZE; i++)
		fsm_tbl[i] = 255;
	fd = open(file_path, O_WRONLY);
	write(fd, fsm_tbl, PAGE_SIZE);
	close(fd);
	return 0;
}

/*
 *	Delete a relation by unlink all segments of it.
 *	Return 0 in case of success and -1 when failed.
 */
int mdUnlink(RelationNode rel){
	int count;
	if((count = mdCount(rel)) < 0)
		return -1;
	char file_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	while(count > 0){										//unlink every segment
		memset(file_path, 0, SYSTEM_PATH_MAX + SYSTEM_NAME_MAX);
		if(constructPath(file_path, rel, count - 1) < 0)
			return -1;
		if (DEBUG)
			printf("[INFO] mdUnlink(): unlink path: %s\n", file_path);
		if(unlink(file_path) < 0){
			return -1;
		}
		count --;
	}
	return 0;
}

/*
 *	Count segment number for a relation.
 *	Return segment number in case of success and -1 when relation did not exist or count failed.
 */
int mdCount(RelationNode rel){
	int count = 0;
	DIR *db_directory; //
	struct dirent *file;
	struct stat file_status;
	char db_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	char file_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	char relation[RELATION_NAME_MAX];
	sprintf(relation, "%d.", rel.relation);
	if(constructPath(db_path, rel, -1) < 0)
		return -1;
	if(( db_directory = opendir(db_path)) == NULL){			//open database path
		if (DEBUG)
			printf("[INFO] mdCount(): cannot open database path %s\n", db_path);
	}
	else{
		while((file = readdir(db_directory)) != NULL){		//count files under database path
			if(strncasecmp(file->d_name, relation, strlen(relation)) != 0 )	//not belong to this relation
				continue;
			sprintf(file_path, "%s/%s", db_path, file->d_name);
			if(stat(file_path, &file_status) >= 0 &&!S_ISDIR(file_status.st_mode)){	//not a directory
					count ++;
			}
		}
	}
	if (DEBUG)
		printf("[INFO] mdCount(): relation count = %d\n", count);
	return count;
}

/*
 *	Open the segment of a relation where the page of "block_num" located in.
 *	Return file handler in case of success and negative number when failed.
 */
int mdOpen(RelationNode rel, int block_num){
	int seg_num;
	if(block_num < 0){										//block is a segment header
		seg_num = -(block_num+1);
	}
	else{
		seg_num = block_num / (MAX_PAGE_NUM - 1);
	}
	char file_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	if(constructPath(file_path, rel, seg_num) < 0)			//construct file path for segment
		return -1;
	if (DEBUG)
		printf("[INFO] mdOpen(): open path: %s\n",file_path);
	int fd = open(file_path, O_RDWR);
	return fd;
}

/*
 *	Close a segment with its file handler.
 */
int mdClose(int fd){
	return close(fd);
}

/*
 *	Read from the segment of a relation where the page of "block_num" located in.
 *	Read result stores in void* buffer.
 *	Return read size in case of success and negative number when failed.
 */
int mdRead(int fd, int block_num, void* buffer, unsigned int size){
	int page_offset;
	if(block_num < 0){										//block is a segment header
		page_offset = 0;
	}
	else{
		page_offset = block_num % (MAX_PAGE_NUM - 1) + 1;
	}
	if(mdSeek(fd, page_offset * PAGE_SIZE) < 0)
		return -1;
//	if (DEBUG)
		printf("[INFO] mdRead(): read from page %d\n", page_offset);
	return read(fd, buffer, size);
}

/*
 *	Write to the segment of a relation where the page of "block_num" located in.
 *	Return write size in case of success and negative number when failed.
 */
int mdWrite(int fd, int block_num, void* buffer, unsigned int size){
	int page_offset;
	if(block_num < 0){										//block is a segment header
		page_offset = 0;
	}
	else{
		page_offset = block_num % (MAX_PAGE_NUM - 1) + 1;
	}
	if(mdSeek(fd, page_offset * PAGE_SIZE) < 0)
		return -1;
	if (DEBUG)
		printf("[INFO] mdWrite(): write to page %d\n", page_offset);
//	printf("......%u.....\n", ((PageHeader*)buffer)->next_tuple);
//	printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//	write(STDOUT_FILENO, (buffer + ((PageHeader*)buffer)->next_tuple), 1056);
//	printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	return write(fd, buffer, size);
}

/*
 * Count used page number of given relation.
 * PageHeader is not included
 */
int mdCountPage(RelationNode rel){
	int count = 0;
	DIR *db_directory;
	struct dirent *file;
	struct stat file_status;
	char db_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	char file_path[SYSTEM_PATH_MAX + SYSTEM_NAME_MAX];
	char relation[RELATION_NAME_MAX];
	sprintf(relation, "%d.", rel.relation);
	if(constructPath(db_path, rel, -1) < 0)		//construct path for given database
		return -1;
	if(( db_directory = opendir(db_path)) == NULL){			//open database path
		if (DEBUG)
			printf("[INFO] mdPageCount(): cannot open database path %s\n", db_path);
	}
	else{
		while((file = readdir(db_directory)) != NULL){		//count files under database path
			if(strncasecmp(file->d_name, relation, strlen(relation)) != 0 )	//not belong to this relation
				continue;
			sprintf(file_path, "%s/%s", db_path, file->d_name);
			if(stat(file_path, &file_status) >= 0 &&!S_ISDIR(file_status.st_mode)){	//not a directory
				count += file_status.st_size / PAGE_SIZE - 1;
			//	if(DEBUG){
					printf("filename = %s, segcount = %d, total_count = %d\n", file_path, file_status.st_size / PAGE_SIZE - 1, count);
					printf("file_status.st_size = %u\n", file_status.st_size);
			//	}
			}
		}
	}
//	if (DEBUG)
		printf("[INFO] mdPageCount(): relation count = %d\n", count);
	return count;
}

/*
 *	Seek an offset from beginning of a file.
 *	Return 0 in case of success and negative number when failed.
 */
int mdSeek(int fd, int offset){
	return lseek(fd, offset, SEEK_SET);
}

/*
 *	Construct path of a database when seg_num < 0 or a segment when seg_num >= 0.
 *	return 0 when succeed and -1 when failed.
 */
int constructPath(char* file_path, RelationNode rel, int seg_num){
	if(strlen(init_path) < SYSTEM_PATH_MAX - DB_NAME_MAX - 2){	//check reserved space for db_name, '/', '\0'
		sprintf(file_path, "%s/%d/", init_path, rel.database);
	}
	else return -1;
	if(seg_num < 0){
		if (DEBUG)
			printf("[INFO] constructPath(): construct path: rel %d seg %d %s\n",rel.relation, seg_num, file_path);
		return 0;								//database path is constructed
	}
	char rel_name[RELATION_NAME_MAX];
	sprintf(rel_name, "%d", rel.relation);
	if(strlen(rel_name) < RELATION_NAME_MAX - 1){			//construct path with relation name
		strcat(file_path, rel_name);
		strcat(file_path, ".");
	}
	else return -1;
	char segment_number[SEG_NAME_MAX];
	sprintf(segment_number, "%d", seg_num);
	if(strlen(segment_number) < SEG_NAME_MAX - 1){			//construct path with segment number
		strcat(file_path, segment_number);
	}
	else return -1;
	if (DEBUG)
		printf("[INFO] constructPath(): construct path: rel %d seg %d %s\n",rel.relation, seg_num, file_path);
	return 0;
}
/*
 *	md.c unit test
 */
/*
int main(int argc, char* argv[]){
	printf("[main]md unit test\n");
	RelationNode rel0 = {0, 0};
	RelationNode rel1 = {0, 1};
	printf("[main]create rel0 result: %d\n", mdCreate(rel0));
	printf("[main]create rel0 again result: %d\n", mdCreate(rel0));
	printf("[main]create rel1 result: %d\n", mdCreate(rel1));
	int fd1, fd2;
	char read_buf[PAGE_SIZE];
	printf("[main]open rel0 block -2 result: %d\n", (fd1 = mdOpen(rel0, -2)));
	printf("[main]read from rel0 block-2 : %d\n", mdRead(fd1, -2, read_buf, PAGE_SIZE));
	int fd = open("output", O_RDWR);
	write(fd, read_buf, PAGE_SIZE);
	close(fd);
	close(fd1);
	printf("[main]close rel0 seg1 result: %d\n", close(fd1));
	char write_buf[PAGE_SIZE];
	unsigned int i = 0;
	while(i < PAGE_SIZE - 1){
		write_buf[i] = i % 26 + 'a';
		i++;
	}
	write_buf[PAGE_SIZE-1] = '\0';
	printf("[main]open rel0 block 8194 result: %d\n", (fd2 = mdOpen(rel0, 8194)));
	printf("[main]write to rel0 block8194 : %d\n",mdWrite(fd2, 8194, write_buf, PAGE_SIZE));
	printf("[main]read from rel0 block8194 : %d\n", mdRead(fd2, 8194, read_buf, PAGE_SIZE));
	printf("[main]read result = %s\n", read_buf);
	char seperate[20] = "-------------------";
	fd = open("/Users/liang/Movies/dbwork/work1/output", O_RDWR|O_APPEND);
	write(fd, seperate, 20);
	write(fd, read_buf, PAGE_SIZE);
	close(fd);
	printf("[main]close rel0 seg2 result: %d\n", close(fd2));
//	printf("delete rel result: %d\n",mdUnlink(rel0));
//	printf("delete rel result: %d\n",mdUnlink(rel1));
	return 0;
}
*/

