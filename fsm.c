/*-------------------------------------------------------------------------
 *
 *	fsm.c
 *
 *	Free Space Management
 *
 *	IDENTIFICATION
 *		src/fsm.c
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"
#include "bufmgr.h"

int freespaceSearch(RelationNode rel, size_t tuple_size){ // return the block_num of page
	unsigned int count = mdCount(rel);
	if(count < 0){
		printf("relation does not exist!\n");
		return -1;
	}
	else{
		unsigned int i = 0;		//**
		while(i < count){		//**
			BufferTag buf_tag;
			buf_tag.rel = rel;
			buf_tag.block_num = -1 - i;
			int bufid = readBuffer(buf_tag, 0);
			unsigned char *seg_fsmtbl = GetBufblockFromId(bufid); 						// to be completed with real read
			unsigned int page_num = 0 ;
			while(page_num < MAX_PAGE_NUM - 1){
				printf("[fsm]search segment %d, page %d, freespace = %d\n", i, page_num, seg_fsmtbl[page_num]*FSM_UNIT_SIZE);
				if(seg_fsmtbl[page_num] == 0)				// page is full
					continue;
				if(seg_fsmtbl[page_num] == PAGE_SIZE / FSM_UNIT_SIZE - 1){		// page is empty
					return page_num;
				}
				else if(tuple_size < (int)seg_fsmtbl[page_num] * FSM_UNIT_SIZE){	// tuple can be contained
					return page_num;
				}
				page_num++;
			}
			i++;
		}
		if(i == count){										// cannot find freespace for this tuple
			return -1;
		}
	}
}
int freespaceUpdate(RelationNode rel, int block_num, size_t tuple_size){
	int seg_num = block_num / (MAX_PAGE_NUM - 1);
	int page_num = block_num % (MAX_PAGE_NUM - 1);
	unsigned char tp_size = tuple_size / FSM_UNIT_SIZE + (tuple_size % FSM_UNIT_SIZE > 0 ? 1:0);
	BufferTag buf_tag;
	buf_tag.rel = rel;
	buf_tag.block_num = -1 - seg_num;
	int bufid = readBuffer(buf_tag, 0);
	unsigned char *seg_fsmtbl = GetBufblockFromId(bufid);								// to be completed with real read
	if(tp_size <= seg_fsmtbl[page_num]){		// (seg_fsmtbl[page_num] - 1) is the first empty unit id
		seg_fsmtbl[page_num] -= tp_size;
		printf("[fsm] seg_num = %d, page_num = %d, freespace = %d\n", seg_num, page_num, seg_fsmtbl[page_num]);
		return 0;
	}
	else{
		printf("tuple cannot be placed in rel %d page %d\n", rel.relation, block_num);
		return -1;
	}
}

int freespacePageVacuum(RelationNode rel, int block_num){
	int seg_num = block_num / (MAX_PAGE_NUM - 1);
	int page_num = block_num % (MAX_PAGE_NUM - 1);
	BufferTag buf_tag;
	buf_tag.rel = rel;
	buf_tag.block_num = -1 - seg_num;
	int bufid = readBuffer(buf_tag, 0);
	unsigned char *page_buf = GetBufblockFromId(bufid);								// to be completed with real read
	PageHeader *phr = (PageHeader *)(page_buf);				// construct page header
	unsigned int item_num = (phr->next_itemid - sizeof(phr)) / sizeof(ItemId);	// count number of item
	unsigned int item_offset = sizeof(phr), valid_item_offset = 0, valid_tuple_offset = PAGE_SIZE;
	bool previous_is_invalid = 0;
	ItemId *iid;											// construct ItemId
	unsigned int i = 0;
	while(i < item_num){
		iid = (ItemId*) (page_buf + item_offset);
		if(iid->tuple_flag == TUPLE_VALID){
			if(previous_is_invalid){
				// move this itemid to front, and start from valid_item_offset
				memcpy(page_buf + valid_item_offset, page_buf + item_offset, sizeof(ItemId));
				// move this tuple to end, and end at valid_tuple_offset
				memcpy(page_buf+valid_tuple_offset - iid->tuple_length, page_buf+iid->tuple_offset, iid->tuple_length);
			}
			valid_item_offset = item_offset + sizeof(ItemId);	// next valid item can be moved to valid_item_offset
			valid_tuple_offset = iid->tuple_offset + iid->tuple_length;	// next valid tuple can be end at valid_tuple_offset
			previous_is_invalid = 0;
		}
		else{
			previous_is_invalid = 1;
		}
		i++;
	}
	phr->invalid_space = 0;
	phr->next_itemid = valid_item_offset;
	phr->next_tuple = valid_tuple_offset;
	buf_tag.rel = rel;
	buf_tag.block_num = block_num;
	bufid = readBuffer(buf_tag, 0);
	unsigned char *seg_fsmtbl = GetBufblockFromId(bufid);
	unsigned int valid_space = (valid_tuple_offset - valid_item_offset);		// calculate freespace in page
	// transform valid_space to fsm unit
	seg_fsmtbl[page_num] = valid_space / FSM_UNIT_SIZE;
	return 0;
}
//
//int main(){
//	unsigned char page_buf[PAGE_SIZE];
//	unsigned char seg_fsmtbl[PAGE_SIZE];
//	unsigned int i = 0;
//	while(i < PAGE_SIZE){
//		seg_fsmtbl[i++] = 255;
//	}
//	RelationNode rel = {0, 0};
//	PageHeader *phr = (PageHeader *)page_buf;
//	phr->invalid_space = 0;
//	phr->next_itemid = 12;
//	phr->next_tuple = PAGE_SIZE;
//	ItemId iid1 = {PAGE_SIZE, TUPLE_VALID, 3000};
//	ItemId iid2 = {PAGE_SIZE - 100, TUPLE_VALID, 3000};
//	memcpy(&(phr->itemid[0]), &iid1, sizeof(iid1));
//	memcpy(page_buf + iid1.tuple_offset - iid1.tuple_length, "1234567890", 10);
//	printf("update iid1\n");
//	freespaceUpdate(rel, 0, 3000 + sizeof(iid1), seg_fsmtbl);
//
//	memcpy(&(phr->itemid[1]), &iid2, sizeof(iid2));
//	memcpy(page_buf + iid2.tuple_offset - iid2.tuple_length, "*****1234567890*****", 20);
//	printf("update iid2\n");
//	freespaceUpdate(rel, 0, 3000 + sizeof(iid2), seg_fsmtbl);
//	printf("[fsm]search a tuple size of tphr %u + tplen %d\n", sizeof(ItemId), 1000);
//	printf("[fsm]search result : page_num = %d\n",freespaceSearch(rel, sizeof(ItemId) + 1000, seg_fsmtbl));
//	return 0;
//}











