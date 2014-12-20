#include "bufmgr.h"
#include "fsm.h"

#define tmp_write_file "/Users/liang/Movies/dbwork/data/tmp_sort"
#define source_file_oper "/home/tangshu/Downloads/src_org/"


unsigned NBuffers = 10;
unsigned NBufTables = 10;
//char init_path[100] = "/home/wcl/Course/DBMS/01/data/";
char init_path[100] = "/Users/liang/Movies/dbwork/data";

bool tmppagecount = 0;

BufferDesc *buffer_descriptors;
char	   *buffer_blocks;
BufferStrategyControl *strategy_control;
BufferHashBucket *buffer_hashtable;
WorkMemory* workmem;
