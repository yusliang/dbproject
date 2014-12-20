#include <stdio.h>

#include "storage.h"
#include "bufmgr.h"

static bool isSamebuf(BufferTag *, BufferTag *);

void initBufTable(int size)
{
	buffer_hashtable = (BufferHashBucket *)malloc(sizeof(BufferHashBucket)*size);
	int i;
	BufferHashBucket *buf_hash = buffer_hashtable;
	for (i = 0; i < size; buf_hash++, i++){
		buf_hash->buf_id = -1;
		buf_hash->hash_key.block_num = -1;
		buf_hash->hash_key.rel.database = -1;
		buf_hash->hash_key.rel.relation = -1;
		buf_hash->next_item = NULL;
//		if (DEBUG)
//			printf("[INFO] Init buf_tag: %u.%u.%u\n",buf_hash->hash_key.rel.relation, buf_hash->hash_key.rel.relation, buf_hash->hash_key.block_num);
	}
}

unsigned buftableHashcode(BufferTag *buf_tag)
{
	unsigned buf_hash = (buf_tag->rel.database+buf_tag->rel.relation+(unsigned)buf_tag->block_num)%NBufTables;
	return buf_hash;
}

int buftableLookup(BufferTag *buf_tag, unsigned hash_code)
{
	if (DEBUG)
		printf("[INFO] Lookup buf_tag: %u.%u.%u\n",buf_tag->rel.database, buf_tag->rel.relation, buf_tag->block_num);
	BufferHashBucket *nowbucket = GetBufHashBucket(hash_code);
//	printf("[buf_table]buf_id = %u\n", nowbucket->buf_id);
//	printf("[buf_table]hashcode = %u\n", hash_code);
	while (nowbucket != NULL) {
	//	printf("nowbucket->buf_id = %u %u %u\n", nowbucket->hash_key.rel.database, nowbucket->hash_key.rel.relation, nowbucket->hash_key.block_num);
		if (isSamebuf(&nowbucket->hash_key, buf_tag)) {
	//		printf("find\n");
			return nowbucket->buf_id;
		}
		nowbucket = nowbucket->next_item;
	}
//	printf("no find\n");

	return -1;
}

int buftableInsert(BufferTag *buf_tag, unsigned hash_code, int buf_id)
{
	if (DEBUG)
		printf("[INFO] Insert buf_tag: %u.%u.%u\n",buf_tag->rel.relation, buf_tag->rel.relation, buf_tag->block_num);
	BufferHashBucket *nowbucket = GetBufHashBucket(hash_code);
	while (nowbucket->next_item != NULL && nowbucket != NULL) {
		if (isSamebuf(&nowbucket->hash_key, buf_tag)) {
			return nowbucket->buf_id;
		}
		nowbucket = nowbucket->next_item;
	}
	if (nowbucket != NULL) {
		BufferHashBucket *newitem = malloc(sizeof(BufferHashBucket));
		newitem->hash_key = *buf_tag;
		newitem->buf_id = buf_id;
		newitem->next_item = NULL;
		nowbucket->next_item = newitem;
	}
	else {
		nowbucket->hash_key = *buf_tag;
		nowbucket->buf_id = buf_id;
		nowbucket->next_item = NULL;
	}

	return -1;
}

int buftableDelete(BufferTag *buf_tag, unsigned hash_code)
{
	if (DEBUG)
		printf("[INFO] Delete buf_tag: %u.%u.%u\n",buf_tag->rel.relation, buf_tag->rel.relation, buf_tag->block_num);
	BufferHashBucket *nowbucket = GetBufHashBucket(hash_code);
	int del_id;
	BufferHashBucket *delitem;
	nowbucket->next_item;
	while (nowbucket->next_item != NULL && nowbucket != NULL) {
		if (isSamebuf(&nowbucket->next_item->hash_key, buf_tag)) {
			del_id = nowbucket->next_item->buf_id;
			break;
		}
		nowbucket = nowbucket->next_item;
	}
	//printf("not found2\n");
	if (isSamebuf(&nowbucket->hash_key, buf_tag)) {
		del_id = nowbucket->buf_id;
	}
	//printf("not found3\n");
	if (nowbucket->next_item != NULL) {
		delitem = nowbucket->next_item;
		nowbucket->next_item = nowbucket->next_item->next_item;
		free(delitem);
		return del_id;
	}
	else {
		delitem = nowbucket->next_item;
		nowbucket->next_item = NULL;
		free(delitem);
		return del_id;
	}

	return -1;
}

static bool isSamebuf(BufferTag *tag1, BufferTag *tag2)
{
	if ((tag1->rel.database != tag2->rel.database) || (tag1->rel.relation != tag2->rel.relation) || (tag1->block_num != tag2->block_num))
		return 0;
	else return 1;
}
