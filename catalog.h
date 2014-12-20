/*
 * catalog.h
 *
 *  Created on: Dec 10, 2014
 *      Author: tangshu
 */

#include "storage.h"
#ifndef SRC_CATALOG_H_
#define SRC_CATALOG_H_
#define INIT_DICT_SIZE 10240

typedef enum
{
	INTTYPE = 0,
	CHARTYPE,
	STRINGTYPE,
	DATETYPE,
	OTHER		//void*
} datatype;

typedef enum
{
	RELATION = 0,
	BTREEINDEX,
	HASHINDEX,
	TMPTABLE
} reltype;

typedef struct
{
	unsigned int attrno;
	char attrname[100];
	datatype attrtype;
	unsigned int length;
} attribute;

typedef struct
{
	RelationNode rel;
	char relname[100];
	reltype type;
	RelationNode owner;
	unsigned int nattr;
	attribute attr[1];
} dbclass;

dbclass* db_class;

void createDict();
dbclass* getDbClass(char* rel_name);
int getTupleSize(dbclass* dc);

extern bool tmppagecount;

#endif /* SRC_CATALOG_H_ */
