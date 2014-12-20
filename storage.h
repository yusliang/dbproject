#ifndef STORAGE_H
#define STORAGE_H

#define SEGMENT_SIZE (PAGE_SIZE*MAX_PAGE_NUM)  //64MB for every segment
#define PAGE_SIZE 8192  //8KB for every page
#define MAX_PAGE_NUM 8192  //8k pages in every segment
#define FSM_UNIT_SIZE 32	//free space mapping use 32bit as minimum unit
#define SYSTEM_PATH_MAX 1024
#define SYSTEM_NAME_MAX 255
#define DB_NAME_MAX 255
#define RELATION_NAME_MAX 120
#define SEG_NAME_MAX 120

#define OID unsigned
#define bool unsigned char
#define size_t unsigned


/* ---------------------------disk management---------------------------- */

typedef struct 
{
	OID 	database;
	OID 	relation;
} RelationNode;

typedef struct 
{
	/*
	RelationNode rel;
	OID segment;
    unsigned usedpage_count;
	*/
    unsigned char 	freespace[MAX_PAGE_NUM];
} SegmentHeader;  //segment description, in the first page of segment

typedef struct 
{
    unsigned 		tuple_offset;
    unsigned 		tuple_length;
    unsigned char 	tuple_flag;
} ItemId;

typedef struct
{
    unsigned 	invalid_space;
    unsigned 	next_itemid;
    unsigned 	next_tuple;
    ItemId 		itemid[1];
} PageHeader;

typedef struct 
{
    unsigned 	attribute_count;
    unsigned 	attribute_length[1];
} TupleHeader;

typedef char Page;

#define TUPLE_VALID 0x01

typedef struct 
{
	RelationNode 	rel;
	int 			block_num;		// block number of relation in physical storage, when block_num<0, -(block_num+1) is segment number of SegmentHeader
} BufferTag;

typedef char WorkMemory;
#define SizeofWorkMemory (10240*1024)	//1M
#define WorkMemCount (SizeofWorkMemory/PAGE_SIZE)
extern WorkMemory* workmem;
extern char init_path[100];

#endif // PAGE_HSTORAGE_H

