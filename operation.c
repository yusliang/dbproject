/*
 * operation.c
 *
 *  Created on: Dec 14, 2014
 *      Author: liang
 */
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include "storage.h"
#include "bufmgr.h"
#include "catalog.h"
#include "operation.h"

#define sort_child_table_tupleNum 10000

/*
 * Begin scan table, including initializing ScanTableDesc, load table blocks to workmem
 * Table blocks can only load less than workmemCount blocks
 */
int begin_scan_table(RelationNode rel, unsigned startBlock, unsigned workmemCount, bool useindex, ScanTableDesc* stdesc){
	printf("Begin table scan!\n");
	if(workmemCount < 0){
		printf("invalid workmemcount!\n");
		return -1;
	}
	printf("startblock:%u workmemcount:%u \n", startBlock, workmemCount);
	if(useindex){
		printf("index not implemented\n");
		stdesc = NULL;
		return -1;
	}
	// full table scan
	stdesc->initialised = 1;
	int block_id;
	BufferTag buf_tag;
	buf_tag.rel = rel;
	stdesc->rel = rel;
	stdesc->curBlock = 0;
	stdesc->curTuple = 0;
	stdesc->workmemBlockCount = workmemCount;
	stdesc->totalBlock = smgrCount(rel);
	stdesc->wmStartBlock = startBlock;
	loadWorkMem(stdesc);
//	int i = 0;
//	//load table to workmem
//	while(i < workmemCount && i < smgrCount(rel)){
//		buf_tag.block_num = i;
//		printf(" buf_tag.block_num = %d\n", buf_tag.block_num);
//		block_id = readBuffer(buf_tag, 0);
//		printf("block_id = %d\n", block_id);
//		printf("getblockfromid = %u\n", ((PageHeader*)GetBufblockFromId(block_id))->next_itemid);
//		memcpy(workmem + (startBlock + i % stdesc->workmemBlockCount)* PAGE_SIZE, GetBufblockFromId(block_id), PAGE_SIZE);
//		printf("workmem offset = %u\n", (startBlock + i % stdesc->workmemBlockCount)* PAGE_SIZE);
//		i++;
//	}
//	stdesc->wmLoadedBlock = i - 1;
	printf("db:%u rel:%u cB:%u cT:%u sB:%u tB:%u wB:%u\n",stdesc->rel.database, stdesc->rel.relation,
						stdesc->curBlock, stdesc->curTuple, stdesc->wmStartBlock, stdesc->totalBlock, stdesc->workmemBlockCount);
//	getchar();
	printf("End Table Scan\n");
	return 0;
}

void loadWorkMem(ScanTableDesc* stdesc){
	//load table to workmem
	int block_id, i = stdesc->curBlock;
	BufferTag buf_tag;
	buf_tag.rel = stdesc->rel;
	//load table to workmem
//	printf("[loadWorkMem]curBlock = %u, totalBlock = %u\n",stdesc->curBlock,stdesc->totalBlock);
//	printf("[loadWorkMem]stdesc->wmLoadedBlock = %u\n", stdesc->wmLoadedBlock);
	while(i < stdesc->curBlock + stdesc->workmemBlockCount && i < stdesc->totalBlock){
//		printf("[loadWorkMem]loading block %d\n", i);
		buf_tag.block_num = i;
		block_id = readBuffer(buf_tag, 0);
//		printf("[loadWorkMem] off = %u\n",(stdesc->wmStartBlock + i % stdesc->workmemBlockCount)* PAGE_SIZE);
		memcpy(workmem + (stdesc->wmStartBlock + i % stdesc->workmemBlockCount)* PAGE_SIZE, GetBufblockFromId(block_id), PAGE_SIZE);
		i++;
	}
	stdesc->wmLoadedBlock = i - 1;

}
int hasBlockToLoad(ScanTableDesc* stdesc){
	if(stdesc->wmLoadedBlock < stdesc->totalBlock - 1){
//		printf("[hasBlockToLoad]true\n");
		return 1;
	}
	return 0;
}

int needLoadWorkMem(ScanTableDesc* stdesc){
//	printf("[needLoadWorkMem]curBlock:%u curTuple:%u totalBlock:%u workBlockCount:%u workLoaded = %u\n",
	//		stdesc->curBlock, stdesc->curTuple, stdesc->totalBlock, stdesc->workmemBlockCount, stdesc->wmLoadedBlock);
	if(stdesc->curBlock == 0 || stdesc->curTuple != 0)
		return 0;
	if(stdesc->curBlock > stdesc->wmLoadedBlock && stdesc->curBlock != 0 && stdesc->curBlock % stdesc->workmemBlockCount == 0){
		return 1;
	}
	return 0;
}

int getWorkMemNext(ScanTableDesc* stdesc, void* tuple){
//	printf("[getWorkMemNext]curBlock:%u curTuple:%u totalBlock:%u workmemBlockCount:%u workLoaded = %u\n",
//				stdesc->curBlock, stdesc->curTuple, stdesc->totalBlock, stdesc->workmemBlockCount, stdesc->wmLoadedBlock);
	if(stdesc->initialised != 1){
		printf("[getWorkMemNext]didn't begin table scan!");
		return -1;
	}
//	printf("[getNext]db:%u rel:%u cB:%u cT:%u tB:%u wB:%u\n",stdesc->rel.database, stdesc->rel.relation,
//			stdesc->curBlock, stdesc->curTuple, stdesc->totalBlock, stdesc->workmemBlockCount);
	PageHeader* buf_off = (PageHeader*)(workmem + (stdesc->wmStartBlock + stdesc->curBlock % stdesc->workmemBlockCount) * PAGE_SIZE);
//	printf("[getNext]workmem = %u, buf_off = %u, buf_off->itemoff = %u\n",workmem, (stdesc->wmStartBlock + stdesc->curBlock % stdesc->workmemBlockCount) * PAGE_SIZE, buf_off->next_itemid);
//	printf("[getNext]buf->next_itemid %u\n", buf_off->next_itemid);
	int tuple_count = getTupleCount(buf_off);
//	printf("[getNext]tuple_count = %d\n", tuple_count);
	if(stdesc->curTuple < 0){
		//printf("[getWorkMemNext]curTuple error!\n");
		return -1;
	}
	if(stdesc->curBlock == stdesc->totalBlock && stdesc->curTuple == 0){
	//	printf("[getWorkMemNext]no next tuple!\n");
		return 1;
	}
	if(stdesc->curBlock > stdesc->wmLoadedBlock){
		return 2;	//should load workmem or ends
	}
	unsigned size;
//	printf("curtuple = %u\n", stdesc->curTuple);
	int err = getTupleFromPage(buf_off, stdesc->curTuple, tuple, &size);
//	printf("[getNext]size = %u\n", size);
//	printf("[getNext]tuple first = %d\n", *((int*)tuple));
	if(err < 0 || size == 0)
		return -1;
	stdesc->curTuple++;	//tuple iterator goes to next one
	//check if needs to goto next block
//	printf("tuple_count = %d\n", tuple_count);
	if(stdesc->curTuple >= tuple_count){
		stdesc->curBlock++;
		stdesc->curTuple = 0;
	}
	return 0;
}

/*
 * reset workmem scanner, set scanner to beginning of workmem
 * this function do not load new blocks to workmem
 */
int resetWmScanner(ScanTableDesc* stdesc){
//	printf("[resetWmScanner]\n");
	if(stdesc->initialised != 1){
		printf("[resetScanner]didn't begin table scan!");
		return -1;
	}
	if(stdesc->wmLoadedBlock == stdesc->totalBlock - 1){	//all blocks has been loaded
		stdesc->curBlock = stdesc->totalBlock - stdesc->totalBlock % stdesc->workmemBlockCount;
	}
	else
		stdesc->curBlock = stdesc->wmLoadedBlock + 1 - stdesc->workmemBlockCount;
//	printf("[resetWmScanner]stdesc->curBlock = %u, stdesc->wmLoadedBlock = %u, stdesc->curBlock = %u, stdesc->totalblock = %u\n"
//			,stdesc->curBlock, stdesc->wmLoadedBlock, stdesc->workmemBlockCount, stdesc->totalBlock);
	stdesc->curTuple = 0;
	return 0;
}

/*
 * reset scanner to beginning
 */
int resetScanner(ScanTableDesc* stdesc){
//	printf("[resetScanner]\n");
	if(stdesc->initialised != 1){
		printf("[resetScanner]didn't begin table scan!");
		return -1;
	}
	stdesc->curBlock = 0;
	stdesc->curTuple = 0;
	stdesc->wmLoadedBlock = 0;
	//load table to workmem
	int i = 0, block_id;
	BufferTag buf_tag;
	buf_tag.rel = stdesc->rel;
	while(i < stdesc->workmemBlockCount && i < smgrCount(stdesc->rel)){
		buf_tag.block_num = i;
		block_id = readBuffer(buf_tag, 0);
		memcpy(workmem + (stdesc->wmStartBlock + i % stdesc->workmemBlockCount)* PAGE_SIZE, GetBufblockFromId(block_id), PAGE_SIZE);
		i++;
	}
	stdesc->wmLoadedBlock = i - 1;
	return 0;
}

/*
 * Get next tuple. If curBlock equal total block number and curTuple is 0, getNext ends and returns 1.
 * If error occurs, return value < 0.
 * If get succeeds, return 0.
 */
int getNext(ScanTableDesc* stdesc, void* tuple){
//	printf("[getNext-begin] stdesc->totalBlock = %u, stdesc->curBlock = %u, stdesc->curTuple = %u\n",
//			stdesc->totalBlock, stdesc->curBlock, stdesc->curTuple);
	if(stdesc->initialised != 1){
		printf("[getNext]didn't begin table scan!");
		return -1;
	}
//	printf("[getNext]db:%u rel:%u cB:%u cT:%u tB:%u wB:%u\n",stdesc->rel.database, stdesc->rel.relation,
//			stdesc->curBlock, stdesc->curTuple, stdesc->totalBlock, stdesc->workmemBlockCount);
	PageHeader* buf_off = (PageHeader*)(workmem + (stdesc->wmStartBlock + stdesc->curBlock % stdesc->workmemBlockCount) * PAGE_SIZE);
//	printf("[getNext]workmem = %u, buf_off = %u, buf_off->itemoff = %u\n",workmem, (stdesc->wmStartBlock + stdesc->curBlock % stdesc->workmemBlockCount) * PAGE_SIZE, buf_off->next_itemid);
//	printf("[getNext]buf->next_itemid %u\n", buf_off->next_itemid);
	int tuple_count = getTupleCount(buf_off);
//	printf("[getNext]tuple_count = %d\n", tuple_count);
	if(stdesc->curTuple < 0){
		printf("curTuple error!\n");
		return -1;
	}
	if(stdesc->curBlock == stdesc->totalBlock && stdesc->curTuple == 0){
		printf("no next tuple!");
		return 1;
	}
	unsigned size;
//	printf("curtuple = %u\n", stdesc->curTuple);
	int err = getTupleFromPage(buf_off, stdesc->curTuple, tuple, &size);
//	printf("[getNext]size = %u\n", size);
//	printf("[getNext]tuple first = %d\n", *((int*)tuple));
	if(err < 0 || size == 0)
		return -1;
	stdesc->curTuple++;	//tuple iterator goes to next one
	//check if needs to goto next block
//	printf("tuple_count = %d\n", tuple_count);
	if(stdesc->curTuple >= tuple_count){
		stdesc->curBlock++;
		stdesc->curTuple = 0;
	}
	//check if new blocks needs to load
//	printf("stdesc->wmLoadedBlock = %u\n", stdesc->wmLoadedBlock);
	if(stdesc->curBlock > stdesc->wmLoadedBlock && stdesc->curBlock != 0 && stdesc->curBlock % stdesc->workmemBlockCount == 0){
		int block_id, i = stdesc->curBlock;
		BufferTag buf_tag;
		buf_tag.rel = stdesc->rel;
//		//load table to workmem
//		printf("%u, %u\n",stdesc->curBlock,stdesc->totalBlock - stdesc->curBlock);
//		printf("stdesc->wmLoadedBlock = %u\n", stdesc->wmLoadedBlock);
		while(i < stdesc->curBlock + stdesc->workmemBlockCount && i < stdesc->totalBlock){
	//		printf("loading block %d\n", stdesc->curBlock);
			buf_tag.block_num = i;
			block_id = readBuffer(buf_tag, 0);
			memcpy(workmem + (stdesc->wmStartBlock + i % stdesc->workmemBlockCount)* PAGE_SIZE, GetBufblockFromId(block_id), PAGE_SIZE);
			i++;
		}
		stdesc->wmLoadedBlock = i - 1;
	}
//	printf("stdesc->wmLoadedBlock = %u\n", stdesc->wmLoadedBlock);
	return 0;
}

int end_scan_table(ScanTableDesc* stdesc){
	if(stdesc != NULL && stdesc){
		free(stdesc);
		return 0;
	}
	return -1;
}

int full_scan_table(char* relname){
	int scan_count = 0;
	dbclass *dbc = (dbclass*)getDbClass(relname);
	int tuple_size = getTupleSize(dbc);
	ScanTableDesc* stdesc = (ScanTableDesc*)malloc(sizeof(ScanTableDesc));
	int err = begin_scan_table(dbc->rel, 0, 3, 0, stdesc);
	if(err < 0){
		return -1;
	}
	char* tuple = (char*)malloc(tuple_size);
	do{
		if(needLoadWorkMem(stdesc)){
			printf("should load work mem\n");
			loadWorkMem(stdesc);
		}
		char tuple1output[100];
		printf("%s, %s, %s, %s\n", dbc->attr[0].attrname, dbc->attr[1].attrname, dbc->attr[2].attrname, dbc->attr[3].attrname);
		while((err = getWorkMemNext(stdesc, tuple)) == 0){
			printf("[full-scan]%d, %d, %d; count = %d\n", *((int*)tuple),  (*((int*)tuple+1)), *((int*)tuple+2), scan_count++);
//			printf("[full-scan]%d, %s; count = %d\n", *(int*)tuple, (char*)(tuple+4), scan_count++);
//			int fd;
//			fd = open("/Users/liang/Movies/dbwork/data/1/out",O_RDWR|O_APPEND);
//			sprintf(tuple1output, "[scan-tuple1]%d \n", *((int*)tuple));
//			write(fd, tuple1output, strlen(tuple1output));
//			close(fd);
		//	getchar();
		}
	}
	while(hasBlockToLoad(stdesc));
	return 0;
}
int simple_join(char* relname1, char* relname2, char* join_key1, char* join_key2, bool flushtemp){
	dbclass *dbc1 = (dbclass*)getDbClass(relname1), *dbc2 = (dbclass*)getDbClass(relname2);
	int tuple_size1 = getTupleSize(dbc1), tuple_size2 = getTupleSize(dbc2);
	int key_off1, key_off2;
	if(findAttrLocation(relname1, join_key1) < 0 || (key_off1 = getAttrOffset(relname1, join_key1)) < 0){
		printf("join_key1 %s is not an attribute of relation 1\n", join_key1);
		return -1;
	}
	if(findAttrLocation(relname2, join_key2) < 0 || (key_off2 = getAttrOffset(relname2, join_key2)) < 0){
		printf("join_key2 %s is not an attribute of relation 2\n", join_key2);
		return -1;
	}
	printf("key_offset1 = %d, key_offset2 = %d\n", key_off1, key_off2);
	char tmptb[20] = {0};
	RelationNode tmprn = {1, 100};
	if(flushtemp){
		smgrUnlink(tmprn);
		createTmpTb(tmptb, relname1, relname2, join_key1, join_key2);
	}
	printf("table2 pass\n");
	//判断大小表，暂时没做
	//load table 1 to workmem with M-1 blocks
	//暂时都用一个块
	int Mjianyi = WorkMemCount - 1;
	printf("M-1 = %d\n", Mjianyi);
// 	getchar();
	ScanTableDesc* scandesc1 = (ScanTableDesc*) malloc(sizeof(ScanTableDesc));
	int err = begin_scan_table(dbc1->rel, 0, 1, 0, scandesc1);
	if(err < 0){
		return 0;
	}
	//load table 2 to workmem with 1 block
	ScanTableDesc* scandesc2 = (ScanTableDesc*) malloc(sizeof(ScanTableDesc));
	err = begin_scan_table(dbc2->rel, 1, Mjianyi, 0, scandesc2);
	if(err < 0){
		return 0;
	}
	ScanTableDesc* predesc = (ScanTableDesc*) malloc(sizeof(ScanTableDesc));
	char* tuple1 = (char*)malloc(tuple_size1);
	char* tuple2 = (char*)malloc(tuple_size2);
	int join_count = 0;
	/*
	 * load table 1 one block at a time, and compare with M-1 blocks of table2 in workmem
	 * if table 1 is full scanned and M-1 blocks are all compared, reset table 1 scanner and load next M-1 blocks of table2
	 */
	char tuple1output[100];
	while(1){
		while((err = getWorkMemNext(scandesc1, tuple1)) == 0){	//get all tuple from table1
			int fd;
			printf("[join-tuple1] %d\n", *((int*)(tuple1 + key_off1)));
			while((err = getWorkMemNext(scandesc2, tuple2)) == 0){ //get in-workmem tuple of table2
				printf("[join-tuple2] %d \n", *((int*)(tuple2 + key_off2)));
				if(*((int*)(tuple1 + key_off1)) == *((int*)(tuple2+key_off2))){
					printf("join!!! count = %d\n", join_count);
					if(flushtemp)
						joinTbTup(tmptb, relname1, relname2, tuple1, tuple2, join_key1, join_key2);
			//		getchar();
//					if(*((int*)tuple1) >= 21770){
//						printf("join!!! count = %d\n", join_count);
//						fd = open("/Users/liang/Movies/dbwork/data/1/out",O_RDWR|O_APPEND);
//						sprintf(tuple1output, "join!!!%d \n", join_count);
//						write(fd, tuple1output, strlen(tuple1output));
//						sprintf(tuple1output, "curBlock = %u, curTuple = %u \n", scandesc1->curBlock, scandesc1->curTuple);
//						write(fd, tuple1output, strlen(tuple1output));
//						close(fd);
//					}
					join_count++;
				}
			}
			memcpy(predesc, scandesc2, sizeof(ScanTableDesc));
			resetWmScanner(scandesc2);
		}
		if(hasBlockToLoad(scandesc1)){
//			printf("table1 has block to load\n");
	//		getchar();
			loadWorkMem(scandesc1);
		}
		else{
			resetScanner(scandesc1);
			if(hasBlockToLoad(scandesc2)){
				printf("table2 has block to load\n");
				printf("[loadWorkMem]curBlock = %u, totalBlock = %u\n",scandesc2->curBlock,scandesc2->totalBlock);
				printf("[loadWorkMem]stdesc->wmLoadedBlock = %u\n", scandesc2->wmLoadedBlock);
				memcpy(scandesc2, predesc, sizeof(ScanTableDesc));
				loadWorkMem(scandesc2);
				printf("table2 has block to load\n");
				printf("[loadWorkMem]curBlock = %u, totalBlock = %u\n",scandesc2->curBlock,scandesc2->totalBlock);
				printf("[loadWorkMem]stdesc->wmLoadedBlock = %u\n", scandesc2->wmLoadedBlock);
				getchar();

			}
			else{
				printf("table2 no block to load, break\n");
				break;
			}
		}
	}
//	while((err = getNext(scandesc1, tuple1)) == 0){
//		printf("[join-tuple1]%d \n", *((int*)tuple1));
//		int tuple2count = 0;
//		//printf("%s\n", tuple1+4);
//		while((err = getNext(scandesc2, tuple2)) == 0){
//		//	printf("[join-tuple2]%d \n", *((int*)tuple2));
//		//	printf("%s\n", tuple2+4);
//			if(*((int*)tuple1) == *((int*)tuple2)){
//				printf("join!!! count = %d\n", join_count++);
//				printf("scanner2 curblock = %u, curTuple = %u\n", scandesc2->curBlock, scandesc2->curTuple);
//				tuple2count ++;
//			}
//		}
//		if(err == 1){
//			printf("reset scanner!!\n");
//			resetScanner(scandesc2);
//		}
//		if(tuple2count > 1)
//			getchar();
//	}
	printf("========joincount = %d=======\n",join_count);
	flushAllBuffers();
//	full_scan_table(tmptb);
	end_scan_table(scandesc1);
	end_scan_table(scandesc2);
}

sort_join(char* relname1, char* relname2, char* join_key1, char* join_key2, bool flushtemp){

}


//
//#define tmp_write_file "/home/tangshu/Downloads/src_org/tmp_sort"
//
//#define source_file_oper "/home/tangshu/Downloads/src_org/"
//
///*int readBuffer_xx(BufferTag bt){
//	int buf_id = readBuffer(bt);
//	void *buf_block = GetBufblockFromId(buf_id);
//	buffer_descriptors[buf_id].buf_flag |= BUF_DIRTY;
//	return buf_id;
//}
//int getRelBlockNum(RelationNode rn){
//	return 5;
//}
//int getTupleNum(PageHeader* ph){
//	int k = 0;
//	for(k = 0;ph->itemid[k].tuple_offset !=ph->next_tuple;k++);
//	return k;
//}
//void* getTupleHeader(void * buf_block,int m){
//	PageHeader *ph = (PageHeader *)buf_block;
//	//TupleHeader *tupHeader = (TupleHeader *)(buf_block+ph->itemid[m].tuple_offset);
//	void *tupHeader = (buf_block+ph->itemid[m].tuple_offset);
//	//char *entry_attribute = (char *) (void *)(buf_block+ph->itemid[m].tuple_offset+sizeof(unsigned)+sizeof(unsigned)*(tupHeader->attribute_count));
//	return tupHeader;
//}
//void* getAttribute(void* tupHeader, int x){
//	TupleHeader * th_tmp = (TupleHeader *)tupHeader;
//
//	int bias = sizeof(th_tmp->attribute_count) + th_tmp->attribute_count* sizeof(unsigned);
//	int i = 0;
//	for(i = 0;i<x;i++)
//		bias +=th_tmp->attribute_length[i];
//
//	void *attr = tupHeader + bias;
//	return attr;
//}
//void* getAttributeHeader(void* tupHeader){
//	TupleHeader * th_tmp = (TupleHeader *)tupHeader;
//
//	int bias = sizeof(th_tmp->attribute_count) + th_tmp->attribute_count* sizeof(unsigned);
//
//	void *attr = tupHeader + bias;
//	return attr;
//}*/
//
//
////1214
//dbclass getDbclass(RelationNode rn){
//	dbclass xx;
//	return xx;
//}
//RelationNode createTable(char* table_name, char** attr_name, int attr_length, datatype* dt){
//	//write catalog
//}
//typedef struct
//{
//    int key;
//    void* value;
//} tuple;
//int* readTableAll(RelationNode rn){
//	int sum = getBlockNum(rn);
//	int buf_id[sum];
//	return buf_id;
//}
//RelationNode createJoinTable(RelationNode rn1, RelationNode rn2){
//	RelationNode res;
//	return res;
//}
//RelationNode join_LoopNest(RelationNode rn1, int Nattr1, RelationNode rn2, int Nattr2){	//block based
//	{
//	dbclass dc1 = getDbclass(rn1);
//	dbclass dc2 = getDbclass(rn2);
//	char* attr_name[dc1.nattr+dc2.nattr-1];
//	int attr_length = dc1.nattr+dc2.nattr-1;
//	datatype dt[dc1.nattr+dc2.nattr-1];
//	int i = 0;
//	for(i = 0; i<dc1.nattr; i++){
//		attr_name[i] = rn1.database+"_"+rn1.relation+"_"+dc1.attr[i].attrname;
//		dt[i] = dc1.attr[i].attrtype;
//	}
//	for(i = 0; i<dc2.nattr; i++){
//		if(i == Nattr2)
//			continue;
//		attr_name[i+dc1.nattr] = rn1.database+"_"+rn1.relation+"_"+dc1.attr[i].attrname;
//		dt[i+dc1.nattr] = dc2.attr[i].attrtype;
//	}
//	RelationNode res = createTable("LoopNest_middle_table", attr_name, attr_length, dt);
//	smgrCreate(res);
//	void* res_buf;int buf_id = 0;
//
//	int bnum1 = getRelBlockNum(rn1);
//	int bnum2 = getRelBlockNum(rn2);
//
//
//	//rn[0] stores the small table
//	RelationNode* rn[2];
//	rn[0] = &rn1;
//	rn[1] = &rn2;
//	if(bnum1 >bnum2){
//		rn[0] = &rn2;
//		rn[1] = &rn1;
//	}
//
//	int bnum_EachTimeRead_1 = bnum1;
//	if(bnum_EachTimeRead_1 > total_block_num-1){
//		bnum_EachTimeRead_1 = total_block_num-1;
//	}
//	int bnum_EachTimeRead_2 = total_block_num - bnum_EachTimeRead_1;
//
//	int readTimes1 = 0;
//	if(bnum1 <= total_block_num-1)
//		readTimes1 = 1;
//	else
//		readTimes1 = (total_block_num-1)/bnum1 +1;
//
//	for(i = 0; i< readTimes1;i++){
//		//read
//		int j = 0;
//		for(j = 0;j<bnum_EachTimeRead_1;j++){
//			if(j+i*bnum_EachTimeRead_1 >=bnum1)
//				break;
//			BufferTag bt = {rn1, j+i*bnum_EachTimeRead_1};
//			readBuffer_xx(bt);
//		}
//		int bnum1_tmp = j;//before the last time of reading r1, it is bnum_EachTimeRead_1-1; it is the remaining...
//		for(j = 0; j< bnum2; j++){
//			BufferTag bt2 = {rn2, j};
//			int buf_id2 = readBuffer_xx(bt2);
//			//join 0-bnum_EachTimeRead_1
//			void *buf_block2 = GetBufblockFromId(buf_id2);
//			PageHeader *ph2 = (PageHeader *)buf_block2;
//			int tuple_num2 = getTupleNum(ph2);
//			int k = 0;
//			for(k = 0;k<tuple_num2;k++){
//				void* th2= getTupleHeader(buf_block2,k);		//assume join key can only be int
//				int key2 = *(int*)getAttribute(th2, Nattr2);
//				int m = 0;
//				for(m = 0; m<bnum1_tmp; m++){
//					BufferTag bt1 = {rn1, j+i*bnum_EachTimeRead_1};
//					int buf_id1 = readBuffer_xx(bt1);
//					void *buf_block1 = GetBufblockFromId(buf_id1);
//					PageHeader *ph1 = (PageHeader *)buf_block1;
//					int tuple_num1 = getTupleNum(ph1);
//					void* th1 = getTupleHeader(buf_block1,m);
//					int key1 = *(int*)getAttribute(th1,Nattr1);
//
//					if(key1 == key2){
//						//output result;
//						int xxx = 0;
//						for (xxx = 0; xxx < dc1.nattr; xxx++) {
//							void* tmp = getAttribute(th1, xxx);
//							strncpy(res_buf, tmp);
//						}
//						for (xxx = 0; xxx < dc2.nattr; xxx++) {
//							if (xxx == Nattr2)
//								continue;
//							void* tmp = getAttribute(th1, xxx);
//							strncpy(res_buf, tmp);
//						}
//
//						if(strlen(res_buf) == 8*1024){//full
//							BufferTag buf_tag = {res, buf_id++};
//							smgrExtend(buf_tag, res_buf);
//							free(res_buf);
//							res_buf = malloc(sizeof(void*)*8*1024);
//						}
//					}
//				}
//			}
//		}
//	}
//	}
//
//	//restart
//	RelationNode res = createJoinTable(rn1,rn2);
//
//	int block_sum_table1= getBlockSum(rn1);
//	int* bufId_table1 = malloc(sizeof(int*)*block_sum_table1);
//	table1 = readTableAll(rn1);
//	int tuple_sum = getTupleSum(rn1);
//	int i = 0;
//	for(i = 0; i< tuple_sum; i++){
//		tuple tt1 = getTupleFromId(bufId_table1, i);
//		int buf_id = initialize(rn2);
//		while(!ReadToEnd(rn2)){
//			tuple tt2 = getNextTuple(rn2);
//			int v1 = getAttr(rn1,Nattr1);
//			int v2 = getAttr(rn1,Nattr2);
//			if(v1 == v2){
//				Print_join(tt1,Nattr1,tt2,Nattr2);
//			}
//		}
//	}
//
//	return res;
//}
//RelationNode MulTableJoin(RelationNode rn[], int rn_length, int No_Attr[]){
//	int i = 0;
//	RelationNode res = rn[0];
//	for(i = 1;i<rn_length;i++){
//		res = join_LoopNest(res,No_Attr[0],rn[i],No_Attr[i]);
//	}
//	return res;
//}
//int Comp(const void *p1, const void *p2){
//	tuple *t1 = (tuple *)p1;
//	tuple *t2 = (tuple *)p2;
//	if(t1->key <t2->key)
//		return -1;
//	if(t1->key == t2->key)
//		return 0;
//	return 1;
//}
//int writeFile_xx(RelationNode res, tuple* tp,int length){
//	void* buf = malloc(sizeof(void*)*1024*8);
//	BufferTag buf_tag = {res,0};
//	smgrExtend(buf_tag, buf);
//	return 0;
//}
//int MultiWay_sort(RelationNode rn1,int table_no){
//	int key_all[1000000];int key_all_sum=0;
//
//	int bnum1 = getBlockNum(rn1);
//	int no = 0;
//	int i = 0;
//	tuple rn1_tuple[sort_child_table_tupleNum];
//	int x = 0;
//	for (i = 0; i < bnum1; i++) {
//		BufferTag bt = { rn1, i };
//		int buf_id = readBuffer_xx(bt);
//		void *buf_block = GetBufblockFromId(buf_id);
//		PageHeader *ph = (PageHeader *) buf_block;
//		int t_num = getTupleNum(ph);
//		int j = 0;
//		for (j = 0; j < t_num; j++) {
//			void* th1 = getTupleHeader(buf_block, j);
//			int k = 0;
//			rn1_tuple[x++] = *(tuple*) getAttributeHeader(th1);
//
//			int kkkey = rn1_tuple[x-1].key;
//			if(!Contains(key_all,kkkey))
//				key_all[key_all_sum++] = kkkey;
//
//			if (x == sort_child_table_tupleNum) {
//				qsort(rn1_tuple, x, sizeof(tuple), Comp);
//				int m = 0;
//				char* file_name = strcat("Sort_middle_table_" + table_no + "_",no);
//				char* attr_name[2] = { "key", "value" };
//				int attr_length = 2;
//				datatype dt[2] = { INTTYPE, OTHER };
//
//				RelationNode res = createTable(file_name, attr_name,
//						attr_length, dt);
//				//FILE* f= fopen(tmp_write_file+"1_"+no, "w+");
//				for(m = 0;m<x;m++){
//				 fwrite(&tuple[m],sizeof(tuple),1,f);
//				 }
//				writeFile_xx(res, rn1_tuple, x);
//				no++;
//				x = 0;
//
//			}
//		}
//	}
//	//return no;
//	qsort(key_all,key_all_sum,sizeof(int),Comp);
//
//	RelationNode res = {100,100};
//	initializeTable(res);
//	RelationNode sub_tables[no];//sub_files
//	void* mem_block[no];
//	tuple tmp_tuple[no];
//	for(i = 0; i < no; i++)
//		initializeMemblock(sub_tables, mem_block);
//	for(i = 0; i < key_all_sum; i++){
//		int j = 0;
//		for (j = 0; j < no; j++){
//			while(!ReadToEnd(sub_tables[j])){
//				if(!IsNull(tmp_tuple[j])){
//					int tt_key = tmp_tuple[j];
//					if(tt_key == key_all[i]){
//						writeTuple(res, tmp_tuple[j]);
//						setNull(&tmp_tuple[j]);
//					}
//					else
//						break;
//				}
//				tuple tup = ReadNextTuple(sub_tables[j]);
//				int ttt_key = tup.key;
//				if(ttt_key == key_all[i])
//					writeTuple(res, tup);
//				else{
//					tmp_tuple[j] = tup;
//					break;
//				}
//			}
//		}
//	}
//	return res;
//
//}
//
//int join_Sort(RelationNode rn1, int Nattr1, RelationNode rn2, int Nattr2){
//	RelationNode sort_table[2] = {MultiWay_sort(rn1,1),MultiWay_sort(rn2,2)};//middle table name
//
//	tuple cur_tup[2][100];int cur_num_table[2] = {0,0};
//
//	tuple tmp_tuple[2] = {ReadNextTuple(sort_table[0]), ReadNextTuple(sort_table[1])};
//
//	int key_all[1000000];int key_all_sum;
//	int i = 0;
//	for (i = 0; i < key_all_sum; i++) {
//		int j = 0;
//		for (j = 0; j < 2; j++) {
//			while (!ReadToEnd(sort_table[j])) {
//				if (!IsNull(tmp_tuple[j])) {
//					int tt_key = tmp_tuple[j];
//					if (tt_key == key_all[i]) {
//						cur_tup[j][cur_num_table[j]++] = tmp_tuple[j];
//						setNull(&tmp_tuple[j]);
//					} else
//						break;
//				}
//				tuple tup = ReadNextTuple(sort_table[j]);
//				int ttt_key = tup.key;
//				if (ttt_key == key_all[i])
//					cur_tup[j][cur_num_table[j]++] = tup;
//				else {
//					tmp_tuple[j] = tup;
//					break;
//				}
//			}
//		}
//
//		if (cur_num_table[0] != 0 && cur_num_table[1] != 0) {
//			//join and write
//
//		}
//	}
//
//
//	int part1 = part_sort(rn1,1);
//	int part2 = part_sort(rn2,1);
//
//	int block_no_file1[part1];//initialize 0
//	int block_no_file2[part2];
//
//	void* part_file1[part1];//initialize malloc(sizeof())
//	void* part_file2[part2];
//
//
//	int tuple_num1[part1];
//	int tuple_num2[part2];
//
//	int cur_p1[part1];//=0
//	int cur_p2[part2];//=0
//
//	tuple cur_t1[1000];int cur_num_t1 = 0;
//	tuple cur_t2[1000];int cur_num_t2 = 0;
//
//	int i = 0;
//	while(1){
//		for(i = 0; i<part1; i++){
//			if(cur_p1[i] == tuple_num1[i]){	//need to read
//				BufferTag bt = {part_sort_table1, block_no_file1[i]++};
//				smgrRead(bt,part_file1[i]);
//				cur_p1[i] = 0;
//			}
//			cur_p1[i] ++;
//			cur_t1[cur_num_t1++] = getNextTuple(part_file1[i],cur_p1[i]);
//			int j = 0;
//			for(j = cur_p1[i]+1; j< tuple_num1[i]; j++){
//				if()
//					break;
//			}
//		}
//	}
//
//	//join
//	for(i = 0;i<part1;i++){
//
//	}
//}
//int join_index(RelationNode rn1, int Nattr1, RelationNode rn2, int Nattr2){
//
//}
//dbclass getDBClass(RelationNode rn){
//
//}
//int getAttrNum(RelationNode rn){
//	dbclass dc = getDBClass(rn);
//	return dc.nattr;
//}
//int Print(RelationNode rn, void* value){
//	dbclass dc = getDBClass(rn);
//	int i = 0;
//	for (i = 0; i < dc.nattr; i++) {
//		switch (dc.attr[i].attrtype) {
//		case INTTYPE:
//			printf("%d", *(int*) value);
//			value+=sizeof(int)*;
//		case CHARTYPE:
//			printf("%d", *(char*) value);
//			value += sizeof(char);
//		}
//	}
//}
//int getTupleSum(void* buf_block){
//
//}
//int read_tuple_no = -1;
//int read_block_no = 0;
//int getBlockSum(RelationNode rn){
//
//}
//int getTupleSum(void* bl){
//
//}
//int ReadToEnd(RelationNode rn, int buf_id){
//	int bl_sum = getBlockSum(rn);
//	void *buf_block = GetBufblockFromId(buf_id);
//	int tp_sum = getTupleSum(buf_block);
//
//	if(read_block_no == bl_sum && read_tuple_no == tp_sum){
//		return 1;
//	}
//	return 0;
//}
//int initializeSelect(RelationNode rn, int buf_id){
//	BufferTag buf_tag = {rn,read_block_no++};
//	buf_id = readBuffer(buf_tag);
//	read_tuple_no = 0;
//	return 1;
//}
//tuple getNextTuple(RelationNode rn, int buf_id){
//
//	void *buf_block = GetBufblockFromId(buf_id);
//	int sum = getTupleSum(buf_block);
//	if(read_tuple_no == sum){
//		BufferTag buf_tag = {rn,read_block_no++};
//		buf_id = readBuffer(buf_tag);
//		read_tuple_no = 0;
//	}
//	tuple tt = getTupleFromId(buf_block, read_tuple_no++);
//}
//int select(RelationNode rn, int key){
//	int used_buf_id = -1;
//	initializeSelect(rn, used_buf_id);
//	while(!ReadToEnd(rn)){
//		tuple tt = getNextTuple(rn,used_buf_id);
//		if(tt.key == key){
//			void* value = tt.value;
//			int attr_num = getAttrNum(rn);
//			printf("%d ",key);
//			Print(rn,value);
//			printf("\n");
//		}
//	}
//	return 1;
//}
//int Print_tuple_attr(tuple tt, int attr_no){
//
//}
//int projection(RelationNode rn, int attr_no){
//	int used_buf_id = -1;
//	initializeSelect(rn, used_buf_id);
//	while (!ReadToEnd(rn)) {
//		tuple tt = getNextTuple(rn, used_buf_id);
//		Print_tuple_attr(tt, attr_no);
//		printf("\n");
//	}
//	return 1;
//}
//
//
//
//typedef struct{
//	int key;
//	int o_key;
//	int t_key;
//	char value[1000];
//}fact;
//typedef struct{
//	int key;
//	char value[1000];
//}ot;
//int simplify_join(char* file1, char* file2,int x1, int x2,char* writefile){
//	//char str[][3] = {"one","fact", "two" };//
//	FILE* f_tmpWrite= fopen(writefile, "w+"); //打开文件;
//
//
//	//char* file_one = strcat(source_file_oper,"one");
//	FILE* f= fopen(file1, "r"); //打开文件;
//	int sum1 = getTupleSum(f);
//	fclose(f);
//
//	f = fopen(file2, "r"); //打开文件;
//	int sum2 = getTupleSum(f);
//	fclose(f);
//
//	ot one[sum];int one_num = 0;
//
//	while (fscanf(f, "%d|%d\n", &one[one_num++].key, one[one_num].value) != EOF);
//	fclose(f);
//
//	char* file_fact = strcat(source_file_oper,"fact");
//	f = fopen(file_fact,"r");
//	fact fact;
//	int i = 0;
//
//	while (fscanf(f, "%d|%d|%d|%s\n", &fact.key,&fact.o_key,&fact.t_key, fact.value) != EOF) {
//		for (i = 1; i < one_num; i++) {
//			if(one[i].key == fact.o_key){
//				fprintf(f_tmpWrite, "%d|%d|%d|%d|",fact.key,fact.o_key,fact.t_key,one[i].key);
//			}
//		}
//	}
//	fclose(f_tmpWrite);
//
//
//	return 1;
//}
//int main(int argc, char *argv[]){
//
//}
