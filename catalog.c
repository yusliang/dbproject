/*
 * catalog.c
 *
 *  Created on: 2014年12月16日
 *      Author: liang
 */
#include <stdio.h>
#include <string.h>
#include "catalog.h"


/*
 * create in-memory catalog (dictionary) for tables
 * here only manually create 3 example tables
 */
void createDict(){
	printf("*****************Create Dictionary*****************\n");
	db_class = (dbclass*)malloc(INIT_DICT_SIZE);
	memset(db_class, 0, INIT_DICT_SIZE);
	unsigned int offset = 0;
	//table one
	((dbclass*) ((char*)db_class+offset))->rel.database = 1;
	((dbclass*) ((char*)db_class+offset))->rel.relation = 0;
	memcpy(((dbclass*) ((char*)db_class+offset))->relname, "one", 4);
	((dbclass*) ((char*)db_class+offset))->type = RELATION;
	((dbclass*) ((char*)db_class+offset))->owner.database = 1;
	((dbclass*) ((char*)db_class+offset))->owner.relation = 0;
	((dbclass*) ((char*)db_class+offset))->nattr = 2;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[0].attrname, "o_key", 6);
	((dbclass*) ((char*)db_class+offset))->attr[0].attrno = 0;
	((dbclass*) ((char*)db_class+offset))->attr[0].attrtype = INTTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[0].length = 4;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[1].attrname, "o_value", 8);
	((dbclass*) ((char*)db_class+offset))->attr[1].attrno = 1;
	((dbclass*) ((char*)db_class+offset))->attr[1].attrtype = STRINGTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[1].length = 1024;
//	printf("%u, %u, %s, %d, %u, %u, %u",
//				((dbclass*) ((char*)db_class+offset))->rel.database,((dbclass*) ((char*)db_class+offset))->rel.relation, ((dbclass*) ((char*)db_class+offset))->relname,
//				((dbclass*) ((char*)db_class+offset))->type, ((dbclass*) ((char*)db_class+offset))->owner.database, ((dbclass*) ((char*)db_class+offset))->owner.relation, ((dbclass*) ((char*)db_class+offset))->nattr);
	printf("table one dict offset = %u ", offset);
	offset = sizeof(dbclass) + sizeof(attribute);
	printf("length = %u\n", offset);

	//table two
	((dbclass*) ((char*)db_class+offset))->rel.database = 1;
	((dbclass*) ((char*)db_class+offset))->rel.relation = 1;
	memcpy(((dbclass*) ((char*)db_class+offset))->relname, "two", 4);
	((dbclass*) ((char*)db_class+offset))->type = RELATION;
	((dbclass*) ((char*)db_class+offset))->owner.database = 1;
	((dbclass*) ((char*)db_class+offset))->owner.relation = 1;
	((dbclass*) ((char*)db_class+offset))->nattr = 2;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[0].attrname, "t_key", 6);
	((dbclass*) ((char*)db_class+offset))->attr[0].attrno = 0;
	((dbclass*) ((char*)db_class+offset))->attr[0].attrtype = INTTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[0].length = 4;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[1].attrname, "t_value", 8);
	((dbclass*) ((char*)db_class+offset))->attr[1].attrno = 1;
	((dbclass*) ((char*)db_class+offset))->attr[1].attrtype = STRINGTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[1].length = 1024;
	printf("table two dict offset = %u ", offset);
	offset += sizeof(dbclass) + sizeof(attribute);
	printf("length = %u\n", offset);

	//table fact
	((dbclass*) ((char*)db_class+offset))->rel.database = 1;
	((dbclass*) ((char*)db_class+offset))->rel.relation = 2;
	memcpy(((dbclass*) ((char*)db_class+offset))->relname, "fact", 5);
	((dbclass*) ((char*)db_class+offset))->type = RELATION;
	((dbclass*) ((char*)db_class+offset))->owner.database = 1;
	((dbclass*) ((char*)db_class+offset))->owner.relation = 2;
	((dbclass*) ((char*)db_class+offset))->nattr = 4;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[0].attrname, "f_key", 6);
	((dbclass*) ((char*)db_class+offset))->attr[0].attrno = 0;
	((dbclass*) ((char*)db_class+offset))->attr[0].attrtype = INTTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[0].length = 4;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[1].attrname, "f_o_key", 8);
	((dbclass*) ((char*)db_class+offset))->attr[1].attrno = 0;
	((dbclass*) ((char*)db_class+offset))->attr[1].attrtype = INTTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[1].length = 4;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[2].attrname, "f_t_key", 8);
	((dbclass*) ((char*)db_class+offset))->attr[2].attrno = 0;
	((dbclass*) ((char*)db_class+offset))->attr[2].attrtype = INTTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[2].length = 4;
	memcpy(((dbclass*) ((char*)db_class+offset))->attr[3].attrname, "f_value", 8);
	((dbclass*) ((char*)db_class+offset))->attr[3].attrno = 1;
	((dbclass*) ((char*)db_class+offset))->attr[3].attrtype = STRINGTYPE;
	((dbclass*) ((char*)db_class+offset))->attr[3].length = 1024;
	printf("table fact dict offset = %u ", offset);
	offset += sizeof(dbclass) + 3 * sizeof(attribute);
	printf("length = %u\n", offset);
}

/*
 * get the start address of dbclass for given table
 */
dbclass* getDbClass(char* rel_name){
//	printf("db_class = %u\n", db_class);
	unsigned offset = 0;
	while(1){
//		printf("offset = %u, nattr = %u, relname = %s, attr2 name = %s\n"
//				, offset, ((dbclass *)((char*)db_class+offset))->nattr,((dbclass *)((char*)db_class+offset))->relname, ((dbclass *)((char*)db_class+offset))->attr[1].attrname);
		if(((dbclass *)((char*)db_class+offset))->nattr <= 0){
			return NULL;
		}
		if(strcmp(((dbclass *)((char*)db_class+offset))->relname, rel_name) == 0){
			return ((char*)db_class+offset);
		}
		else{
			offset += sizeof(dbclass) + (((dbclass *)((char*)db_class+offset))->nattr-1) * sizeof(attribute);
		}
	}
}

int getTupleSize(dbclass* dc){
	int i = 0, size = 0;
	while(i < dc->nattr){
//		printf("rel %s attr[%d].length = %u ", dc->relname, i, dc->attr[i].length);
		size += dc->attr[i].length;
		i++;
	}
	printf("\n");
	return size;
}

int findAttrLocation(char *relname, char *attrname)
{
//	printf("in findAttrLocation, relname=%s\n", relname);
	dbclass *rel = getDbClass(relname);
	int i;
	for (i=0; i<rel->nattr; i++){
		if(strcmp(rel->attr[i].attrname, attrname) == 0){
			return i;
		}
	}
	return -1;
}

int getAttrOffset(char *relname, char *attrname)
{
	printf("in getAttrOffset, relname=%s\n", relname);
	dbclass *rel = getDbClass(relname);
	int i, offset = 0;
	for (i=0; i<rel->nattr; i++){
		if(strcmp(rel->attr[i].attrname, attrname) != 0){
			offset += rel->attr[i].length;
		}
		else break;
	}
	if(i >= rel->nattr)
		offset = -1;
	return offset;
}

void createTmpTb(char *tmptb, char *relname1, char *relname2, char *join_key1, char *join_key2)
{
	int t = 100;
	//time(&t);
	sprintf(tmptb,"%d",t);
	printf("in createTmpTb, tmptb=%s\n", tmptb);

	dbclass *rel1 = getDbClass(relname1);
	dbclass *rel2 = getDbClass(relname2);
	unsigned offset = 0;
	while(1){
//		printf("offset = %u, nattr = %u, relname = %s, attr2 name = %s\n"
//				, offset, ((dbclass *)((char*)db_class+offset))->nattr,((dbclass *)((char*)db_class+offset))->relname, ((dbclass *)((char*)db_class+offset))->attr[1].attrname);
//		printf("\n");
		if(((dbclass *)((char*)db_class+offset))->nattr <= 0){
			break;
		}
		offset += sizeof(dbclass) + (((dbclass *)((char*)db_class+offset))->nattr-1) * sizeof(attribute);
	}
	((dbclass*) ((char*)db_class+offset))->rel.database = 1;
	((dbclass*) ((char*)db_class+offset))->rel.relation = t;
	memcpy(((dbclass*) ((char*)db_class+offset))->relname, tmptb, strlen(tmptb));
	printf("%d tmptb=%s\n", strlen(tmptb), ((dbclass*) ((char*)db_class+offset))->relname);
	((dbclass*) ((char*)db_class+offset))->type = TMPTABLE;
	((dbclass*) ((char*)db_class+offset))->owner.database = 1;
	((dbclass*) ((char*)db_class+offset))->owner.relation = -1;
	((dbclass*) ((char*)db_class+offset))->nattr = rel1->nattr+rel2->nattr-1;
	int i;
	for (i=0; i<rel1->nattr; i++){
		((dbclass*) ((char*)db_class+offset))->attr[i] = rel1->attr[i];
		printf("newtmp attr[%d]=%s\n", i,((dbclass*) ((char*)db_class+offset))->attr[i].attrname);
	}
	int j=i;
	for (i=0; i<rel2->nattr; i++)
		if(strcmp(rel2->attr[i].attrname, join_key2) != 0){
			((dbclass*) ((char*)db_class+offset))->attr[j] = rel2->attr[i];
			printf("newtmp attr[%d]=%s\n", j,((dbclass*) ((char*)db_class+offset))->attr[j].attrname);
			j++;
		}

	tmppagecount = 0;

	smgrCreate(((dbclass*) ((char*)db_class+offset))->rel);
}

void dropTmpTb(char *tmptb)
{
	dbclass *tmp = getDbClass(tmptb);
	tmp->nattr = 0;
	tmppagecount = 0;
	smgrUnlink(tmp->rel);
}
