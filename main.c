/*
 * main.c
 *
 *  Created on: 2014年12月14日
 *      Author: liang
 */

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include "storage.h"
#include "catalog.h"
#include "bufmgr.h"
#include "operation.h"
#include "smgr.h"
#include "page.h"

#define load_data_directory "/Users/liang/Movies/dbwork/dbms_src/src_org/src/Default/"

/*
 * load data to table with the same file name,
 * split column with "|"
 */
int loadData(char* rel_name){
	char input_line[1200], integer_array[12];
	char input_filename[200];
	char* tuple;
	int i, j, k, integer, buf_id;
	unsigned nattr, tuple_size = 0, attr_off;
	BufferTag buf_tag;
	PageHeader* buf_block;

	dbclass *table_class = getDbClass(rel_name);
	buf_tag.rel = table_class->rel;
	buf_tag.block_num = 0;
	nattr = table_class->nattr;
	for(i = 0; i < nattr; i++){
		tuple_size += table_class->attr[i].length;
	}
//	printf("db = %u, rel = %u, nattr = %d, tuple_size = %u\n", buf_tag.rel.database, buf_tag.rel.relation, nattr, tuple_size);
	if(tuple_size > 0)
		tuple = (char*)malloc(tuple_size);
	else return -1;

	//open data file
	sprintf(input_filename, "%s%s", load_data_directory, rel_name);
	FILE* input = fopen(input_filename, "r");
	if(input == NULL){
		printf("cannot open data file %s\n", rel_name);
		return -1;
	}

	buf_id = readBuffer(buf_tag, 1);
	while(fscanf(input,"%s",input_line) == 1){
		buf_id = readBuffer(buf_tag, 0);
		buf_block = (Page*)GetBufblockFromId(buf_id);
//		printf("buf_block = %u\n", buf_block);
//		printf("[main]%u, %u, %u\n",
//					((PageHeader*)buf_block)->invalid_space,((PageHeader*)buf_block)->next_itemid,((PageHeader*)buf_block)->next_tuple);
		memset(tuple, 0 , tuple_size);
		printf("line:%s\n", input_line);
		j = k = 0;
		attr_off = 0;
		for(i = 0; i < nattr; i++){
			if( table_class->attr[i].attrtype == INTTYPE){		//取出整数
				for(; input_line[j] != '|' && input_line[j] != '\0'; j++);
//				printf("j = %d k = %d\n", j,k);
				memcpy(integer_array, input_line + k, j - k);
				integer_array[j] = '\0';
				integer = atoi(integer_array);
//				printf("array = %s, integer = %d, attr_off = %u\n", integer_array, integer, attr_off);
				(*(int*)(tuple + attr_off)) = integer;
//				printf("....%d\n", *tuple);
				attr_off += table_class->attr[i].length;
//				printf("+++++++++++++++++++++++++\n");
//				write(STDOUT_FILENO,(char *)tuple, tuple_size);
//				printf("\n+++++++++++++++++++++\n");
			}
			else if(table_class->attr[i].attrtype == STRINGTYPE){
//				printf("attr_off = %u\n", attr_off);
				memcpy((char*)(tuple + attr_off), input_line + k, table_class->attr[i].length);
//				printf("....%d\n", *tuple);
//				printf("str = %s\n", (char*)(tuple + attr_off));
//				printf("--------------------------\n");
//				write(STDOUT_FILENO,(char *)tuple, tuple_size);
//				printf("\n----------------------\n");
			}
			k = ++j;
		}
		getchar();
//		if(buf_tag.block_num % 2 == 0)
//		{
//			getchar();
//			printf("test\n");
//		}
		if(pageInsertTuple(buf_block, tuple, tuple_size) < 0){
//			printf("no space in block %u\n", buf_tag.block_num);
			buf_tag.block_num++;
			buf_id = readBuffer(buf_tag, 1);
			buf_block = (PageHeader*)GetBufblockFromId(buf_id);
			if(pageInsertTuple(buf_block, tuple, tuple_size) < 0){
				printf("insert again failed\n");
				return -1;
			}
		}
		buffer_descriptors[buf_id].buf_flag |= BUF_DIRTY;
	}
	printf("read ends!!\n");
	flushAllBuffers();
	printf("\nLoad data ends\n");
	return 0;
}

int main(){
	createDict();
	initBuffer();
	dbclass* dbc1 = getDbClass("two");
	printf("%u\n", dbc1);
	printf("%u %u %u %u\n", dbc1->rel.database, dbc1->rel.relation, dbc1->nattr, dbc1->attr[1].length);
	//create table for table one, two & fact
//	smgrUnlink(getDbClass("one")->rel);
//	smgrCreate(getDbClass("one")->rel);
//	loadData("one");
//	smgrUnlink(getDbClass("two")->rel);
//	smgrCreate(getDbClass("two")->rel);
//	loadData("two");
//	smgrUnlink(getDbClass("fact")->rel);
//	smgrCreate(getDbClass("fact")->rel);
//	loadData("fact");
	char* buf[8192];
	RelationNode rel = {1, 2};
	BufferTag tag;
	tag.rel = rel;
	tag.block_num = 100;
	printf("smgrread = %d\n", smgrRead(tag, buf));




//	RelationNode rel = {1, 0};
//	BufferTag buf_tag;
//	buf_tag.rel = rel;
//	buf_tag.block_num = 0;
//	printf("*******************\n");
//	int block_id = readBuffer(buf_tag, 0);
//	Page* buffer = (Page*)GetBufblockFromId(block_id);
//	printf("%u ... %u\n", buffer, buffer_blocks);
//	printf("%u...\n", ((PageHeader*)buffer)->itemid[0].tuple_offset);
//	printf("%d\n", *(int*)(buffer + ((PageHeader*)buffer)->itemid[0].tuple_offset));
//	ScanTableDesc* scandesc = (ScanTableDesc*) malloc(sizeof(ScanTableDesc));
//	int err = begin_scan_table(rel, 0, 1, 0, scandesc);
//	if(err < 0){
//		return 0;
//	}
//	printf("db:%u rel:%u cB:%u cT:%u tB:%u wB:%u\n",scandesc->rel.database, scandesc->rel.relation,
//				scandesc->curBlock, scandesc->curTuple, scandesc->totalBlock, scandesc->workmemBlockCount);
//	char tuple[1200];
//	while((err = getNext(scandesc, tuple)) == 0){
//		printf("[main]%d %100s\n", *((int*)tuple), (tuple+4));
//	}
//	printf("err = %d\n", err);
//	end_scan_table(scandesc);

//	simple_join("fact", "one", "f_o_key", "o_key", 1);
//	simple_join("100", "two", "f_o_key", "t_key", 0);
//	simple_join("fact", "one", "f_o_key", "o_key");
//	full_scan_table("100");
//	return 0;
}




