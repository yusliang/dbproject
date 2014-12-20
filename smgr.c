#include <stdio.h>

#include "storage.h"
#include "smgr.h"

int smgrCreate(RelationNode rel)
{
	return mdCreate(rel);
}

int smgrUnlink(RelationNode rel)
{
	return mdUnlink(rel);
}

int smgrCount(RelationNode rel){
	return mdCountPage(rel);
}

int smgrRead(BufferTag buf_tag, void* buf)
{
	int fd = mdOpen(buf_tag.rel, buf_tag.block_num);
	int result = mdRead(fd, buf_tag.block_num, buf, PAGE_SIZE);
	mdClose(fd);
	return result;
}

int smgrWrite(BufferTag buf_tag, void* buf)
{
	int fd = mdOpen(buf_tag.rel, buf_tag.block_num);
	int result = mdWrite(fd, buf_tag.block_num, buf, PAGE_SIZE);
	mdClose(fd);
	return result;
}

int smgrExtend(BufferTag buf_tag, void* buf)
{
	if (buf_tag.block_num % MAX_PAGE_NUM == 0) {
		mdCreate(buf_tag.rel);
	}
	int fd = mdOpen(buf_tag.rel, buf_tag.block_num);
	int result = mdWrite(fd, buf_tag.block_num, buf, PAGE_SIZE);
	mdClose(fd);
	return result;
}
