/*
 * page.c
 *
 *  Created on: 2014年12月15日
 *      Author: liang
 */

#include <unistd.h>
#include <fcntl.h>
#include "storage.h"
#include "catalog.h"
#include "bufmgr.h"
#include "page.h"

int pageInsertTuple(Page* page, void* buf, unsigned size){
//	printf("In PageInsertTuple\n");
	PageHeader* phdr = (PageHeader*) page;
//	printf("phdr = %u, invalid = %u, nextitem=%u, nexttuple=%u, pagehdrsize = %u, sizeofitemid = %u\n",
//			phdr, phdr->invalid_space, phdr->next_itemid, phdr->next_tuple, SizeofPageHeader, sizeof(ItemId));

//	int fd;
//	fd = open("/Users/liang/Movies/dbwork/data/1/out",O_RDWR|O_APPEND);
//	write(fd, phdr, PAGE_SIZE);
//	write(fd, "\n\n************\n\n", 16);
//	close(fd);
//	printf("tuple size = %u\n", size);
	printf("page size = %u\n", phdr->next_tuple - phdr->next_itemid -sizeof(ItemId));
	if(phdr->next_tuple - phdr->next_itemid -sizeof(ItemId) < size){
		printf("not enough space!\n");
		return -1;
	}
//	printf("page = %u, page->next_itemid = %u, nextiem = %u, offset = %u, flag = %u, len = %u",
//			page, phdr->next_itemid,page + phdr->next_itemid, &(((ItemId*)(page + phdr->next_itemid))->tuple_offset),
//			&(((ItemId*)(page + phdr->next_itemid))->tuple_flag), &(((ItemId*)(page + phdr->next_itemid))->tuple_length));
	((ItemId*)(page + phdr->next_itemid))->tuple_offset = phdr->next_tuple - size;
	((ItemId*)(page + phdr->next_itemid))->tuple_flag = TUPLE_VALID;
	((ItemId*)(page + phdr->next_itemid))->tuple_length = size;
//	printf("invalid = %u, nextitem=%u, nexttuple=%u\n",
//				phdr->invalid_space, phdr->next_itemid, phdr->next_tuple);

//	printf("nextiem = %u, offset = %u, flag = %u, len = %u\n",
//				page + phdr->next_itemid, &(((ItemId*)(page + phdr->next_itemid))->tuple_offset),
//				&(((ItemId*)(page + phdr->next_itemid))->tuple_flag), &(((ItemId*)(page + phdr->next_itemid))->tuple_length));
//	printf("%u, %u, %u\n", ((ItemId*)(page + phdr->next_itemid))->tuple_offset, ((ItemId*)(page + phdr->next_itemid))->tuple_flag,((ItemId*)(page + phdr->next_itemid))->tuple_length);
//	fd = open("/Users/liang/Movies/dbwork/data/1/out",O_RDWR|O_APPEND);
//	write(fd, page, PAGE_SIZE);
//	write(fd, "\n\n============\n\n", 16);
//	close(fd);
//	printf("next_tuple - size = %u\n", phdr->next_tuple - size);
	memcpy((page + phdr->next_tuple - size), buf, size);
//	printf("memcpyadd = %u\n",page + phdr->next_tuple - size);
//	fd = open("/Users/liang/Movies/dbwork/data/1/out",O_RDWR|O_APPEND);
//	write(fd, page, PAGE_SIZE);
//	write(fd, "\n\n------------\n\n", 16);
//	close(fd);
	phdr->next_itemid += sizeof(ItemId);
	phdr->next_tuple -= size;
//	printf("End PageInsertTuple\n");
	return 0;
}

int getTupleCount(Page* page){
	PageHeader* phdr = (PageHeader*) page;
	if(phdr->next_itemid <= SizeofPageHeader){
		return 0;
	}
	if((phdr->next_itemid - SizeofPageHeader) % sizeof(ItemId) != 0){
		printf("Page corrupted\n");
		return -1;
	}
//	printf("[getTupleCount] next_itemid = %u\n",phdr->next_itemid);
	int count = (phdr->next_itemid - SizeofPageHeader) / sizeof(ItemId);
	return count;
}

int getTupleFromPage(Page* page, int tuplenum, void* tuple, unsigned* size){
	if(tuplenum < 0 || tuplenum > getTupleCount(page)){
		printf("tuplenum error! %d\n", tuplenum);
		return -1;
	}
	ItemId* id = (ItemId*)(page + SizeofPageHeader + sizeof(ItemId) * tuplenum);
	memcpy(tuple, (page + id->tuple_offset), id->tuple_length);
	*size = id->tuple_length;
	return 0;
}

void joinTbTup(char *tmptb, char *relname1, char *relname2, char *tuple1, char *tuple2, char *join_key1, char *join_key2)
{
//	printf("in joinTbTup\n");
	int join_key_n1 = findAttrLocation(relname1, join_key1);
	int join_key_n2 = findAttrLocation(relname2, join_key2);
//	printf("key1=%d key2=%d\n", join_key_n1, join_key_n2);
	dbclass *rel1 = getDbClass(relname1);
	dbclass *rel2 = getDbClass(relname2);
	char *newtuple = malloc(getTupleSize(rel1) + getTupleSize(rel2));
	int offset1 = 0, offset2 = 0;
	memcpy(newtuple, tuple1, getTupleSize(rel1));
	offset1 += getTupleSize(rel1);
	int i, j=0;
	for (i=0; i<rel2->nattr; i++){
		if (i != join_key_n2){
			memcpy(newtuple+offset1, tuple2+offset2, rel2->attr[i].length);
			j++;
			offset1 += rel2->attr[i].length;
		}
		offset2 += rel2->attr[i].length;
	}
//	printf("newtuple[0]=%d\n", *(int *)newtuple);
//	printf("newtuple[1]=%s\n", newtuple+4);
//	printf("newtuple[2]=%s\n", newtuple+rel1->attr[0].length+rel1->attr[1].length);

	dbclass *tmp = getDbClass(tmptb);
	BufferTag buf_tag;
	buf_tag.rel = tmp->rel;
	int buf_id;
//	printf("mdCountPage(tmp->rel)=%d\n", mdCountPage(tmp->rel));
	if (tmppagecount > 0){
		buf_tag.block_num = tmppagecount - 1;
		buf_id = readBuffer(buf_tag, 0);
	}else{
		buf_tag.block_num = 0;
		tmppagecount++;
		buf_id = readBuffer(buf_tag, 1);
	}
	PageHeader* buf_block = (Page*)GetBufblockFromId(buf_id);
//	printf("block_number = %u tuplesize = %u\n", buf_tag.block_num, getTupleSize(tmp));
	if(pageInsertTuple(buf_block, newtuple, getTupleSize(tmp)) < 0){
		printf("no space in block %u\n", buf_tag.block_num);
//		flushBuffer(&buffer_descriptors[buf_id]);
		buf_tag.block_num++;
		tmppagecount++;
//		printf("block_num = %u tuplesize = %u\n", buf_tag.block_num, getTupleSize(tmp));
		buf_id = readBuffer(buf_tag, 1);
		buf_block = (PageHeader*)GetBufblockFromId(buf_id);
		if(pageInsertTuple(buf_block, newtuple, getTupleSize(tmp)) < 0){
			printf("insert again failed\n");
			return ;
		}
	}

}

