#include <stdio.h>
#include <stdlib.h>

#include "storage.h"
#include "bufmgr.h"
#include "fsm.h"
#include "buf_table.h"

static BufferDesc *bufferAlloc(BufferTag buf_tag, bool *found);
static BufferDesc *getStrategyBuffer();
static void freeStrategyBuffer(BufferDesc *buf_hdr);

/*
 * init buffer hash table, strategy_control, buffer, work_mem
 */
void initBuffer()
{
	initBufTable(NBufTables);

	strategy_control = (BufferStrategyControl *) malloc(sizeof(BufferStrategyControl));
	strategy_control->first_freebuffer = 0;
	strategy_control->last_freebuffer = NBuffers - 1;
	strategy_control->next_victimbuffer = 0;

	buffer_descriptors = (BufferDesc *) malloc(sizeof(BufferDesc)*NBuffers);
	BufferDesc *buf_hdr;
	int i;
	buf_hdr = buffer_descriptors;
	for (i = 0; i < NBuffers; buf_hdr++, i++) {
		buf_hdr->buf_flag = 0;
		buf_hdr->buf_id = i;
		buf_hdr->usage_count = 0;
		buf_hdr->next_freebuf = i + 1;
	}
	buffer_descriptors[NBuffers - 1].next_freebuf = -1;

	buffer_blocks = (char *) malloc(PAGE_SIZE*NBuffers);
	memset(buffer_blocks, 0, PAGE_SIZE*NBuffers);

	//init work_mem
	workmem = (WorkMemory*)malloc(SizeofWorkMemory);
}

/*
 * readBuffer--return the buf_id of buffer according to buf_tag
 */
int readBuffer(BufferTag buf_tag, bool isExtend)
{
	void *buf_block;
	bool found;

	static BufferDesc *buf_hdr;
	if (DEBUG)
		printf("[INFO] readBuffer():-------buf_tag.block_num=%d\n", buf_tag.block_num);
	buf_hdr = bufferAlloc(buf_tag, &found);
	if (found) {
		buf_hdr->usage_count++;
		return buf_hdr->buf_id;
	}
	else {
		buf_hdr->usage_count = 1;
		buf_block = GetBufblockFromId(buf_hdr->buf_id);
		smgrRead(buf_tag, buf_block);
		if (isExtend) {
			memset(buf_block, 0, PAGE_SIZE);
			PageHeader *page_hdr = (PageHeader *) buf_block;
			page_hdr->next_tuple = PAGE_SIZE - 1;
			page_hdr->next_itemid = sizeof(PageHeader) - sizeof(ItemId);
		}
	}
	buf_hdr->buf_flag |= BUF_VALID;

	return buf_hdr->buf_id;
}

/*
 * bufferAlloc--return a allocated buffer or a empty buffer which can be write
 */
static BufferDesc *bufferAlloc(BufferTag buf_tag, bool *found)
{
	BufferDesc *buf_hdr;
	unsigned buf_hash = buftableHashcode(&buf_tag);
	int buf_id = buftableLookup(&buf_tag, buf_hash);

	if (buf_id >= 0) {
		buf_hdr = &buffer_descriptors[buf_id];
		*found = 1;
		return buf_hdr;
	}

	buf_hdr = getStrategyBuffer();

	unsigned char old_flag = buf_hdr->buf_flag;
	BufferTag old_tag = buf_hdr->buf_tag;
	if (DEBUG)
		printf("[INFO] bufferAlloc(): old_flag&BUF_DIRTY=%d\n", old_flag & BUF_DIRTY);
	if (old_flag & BUF_DIRTY != 0) {
		flushBuffer(buf_hdr);
	}
	if (old_flag & BUF_VALID != 0) {
		unsigned old_hash = buftableHashcode(&old_tag);
		buftableDelete(&old_tag, old_hash);
	}
	buftableInsert(&buf_tag, buf_hash, buf_hdr->buf_id);
	buf_hdr->buf_flag &= ~(BUF_VALID | BUF_DIRTY);
	buf_hdr->buf_tag = buf_tag;
	*found = 0;

	return buf_hdr;
}

void invalidBuffer(BufferDesc *buf_hdr)
{
	unsigned buf_hash = buftableHashcode(&buf_hdr->buf_tag);
	int buf_id = buftableLookup(&buf_hdr->buf_tag, buf_hash);

	if (buf_id >= 0) {
		buftableDelete(&buf_hdr->buf_tag, buf_hash);
		freeStrategyBuffer(buf_hdr);
		buf_hdr->buf_flag = 0;
		buf_hdr->usage_count = 0;
	}

}

static BufferDesc *getStrategyBuffer()
{
	BufferDesc *buf_hdr;

	if (strategy_control->first_freebuffer >=0 ) {
		buf_hdr = &buffer_descriptors[strategy_control->first_freebuffer];
		strategy_control->first_freebuffer = buf_hdr->next_freebuf;
		buf_hdr->next_freebuf = -1;
		return buf_hdr;
	}

	for (;;) {
		buf_hdr = &buffer_descriptors[strategy_control->next_victimbuffer];
		strategy_control->next_victimbuffer++;
		if (strategy_control->next_victimbuffer >= NBuffers) {
			strategy_control->next_victimbuffer = 0;
		}
		if (buf_hdr->usage_count > 0) {
			buf_hdr->usage_count--;
		}
		else
			return buf_hdr;
	}

	return NULL;
}

static void freeStrategyBuffer(BufferDesc *buf_hdr)
{
	if (buf_hdr->next_freebuf == -1)
	{
		buf_hdr->next_freebuf = strategy_control->first_freebuffer;
		if (buf_hdr->next_freebuf < 0)
			strategy_control->last_freebuffer = buf_hdr->buf_id;
		strategy_control->first_freebuffer = buf_hdr->buf_id;
	}
}

void flushBuffer(BufferDesc *buf_hdr)
{
	smgrWrite(buf_hdr->buf_tag, GetBufblockFromId(buf_hdr->buf_id));
}

void flushAllBuffers()
{
	unsigned int i = 0;
	for (i=0; i<NBuffers; i++)
	{
		printf("flush buffer %d\n", i);
		flushBuffer(&buffer_descriptors[i]);
	}
}

