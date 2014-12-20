/*
 * page.h
 *
 *  Created on: 2014年12月15日
 *      Author: liang
 */

#ifndef PAGE_H_
#define PAGE_H_

#define SizeofPageHeader (sizeof(PageHeader) - sizeof(ItemId))
int pageInsertTuple(Page* page, void* buf, unsigned size);
int getTupleCount(Page* page);
int getTupleFromPage(Page* page, int tuplenum, void* tuple, unsigned* size);

extern bool tmppagecount;

#endif /* PAGE_H_ */
