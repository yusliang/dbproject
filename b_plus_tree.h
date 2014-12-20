/*
 * b_plus_tree.h
 *
 *  Created on: Nov 2, 2014
 *      Author: tangshu
 */

#ifndef SRC_B_PLUS_TREE_H_
#define SRC_B_PLUS_TREE_H_

#define MAX 5
#define MIN 3

#define NODE 0
#define LEAF 1

#include "storage.h"

typedef struct{
	int key;
	BufferTag bt;
	int ItemID;	//non-leaf node set as -1
}Record;

typedef struct{
	int type;
	int count;
	Record pair[MAX+2];
	int parent;
}Node;

typedef struct
{
    int record_num;
    int type;
    int count;
    int parent;
} PageHeader_BIndex;

int createBTree(RelationNode table, int KeyNo);	//key number
int searchBTree(RelationNode table, int KeyNo, int KeyValue, Record *res);	//key value
int insertBTree(RelationNode table, Record rr);
int deleteBTree(RelationNode table, Record rr);

#endif /* SRC_B_PLUS_TREE_H_ */
