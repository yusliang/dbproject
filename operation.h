/*
 * operation.h
 *
 *  Created on: 2014年12月15日
 *      Author: liang
 */

#ifndef OPERATION_H_
#define OPERATION_H_

typedef struct ScanTableDesc{
	RelationNode rel;
	unsigned workmemBlockCount;	// how much blocks of workmem are alloced for this table
	unsigned wmStartBlock;	//from which block is alloced for this table
	unsigned wmLoadedBlock; //until which block is loaded to workmem
	unsigned totalBlock;	//currently how much blocks are in workmem
	unsigned curBlock;		//currently iterator is going to read No.curBlock block, count limits in one relation
	unsigned curTuple;		//currently iterator is going to read No.curTuple tuple, count limits in one block
	bool initialised;		//if this descriptor is initialized
	bool needloading;
}ScanTableDesc;

int begin_scan_table(RelationNode rel, unsigned startBlock, unsigned workmemCount, bool useindex, ScanTableDesc* stdesc);
int full_scan_table(char* relname);
int simple_join(char* relname1, char* relname2, char* join_key1, char* join_key2, bool flushtemp);

void loadWorkMem(ScanTableDesc* stdesc);

#endif /* OPERATION_H_ */
