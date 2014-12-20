#ifndef SMGR_H
#define SMGR_H

extern int smgrCreate(RelationNode);
extern int smgrUnlink(RelationNode);
extern int smgrCount(RelationNode);
extern int smgrRead(BufferTag, void*);
extern int smgrWrite(BufferTag, void*);
extern int smgrExtend(BufferTag, void*);

extern int mdCreate(RelationNode rel);
extern int mdUnlink(RelationNode rel);
extern int mdOpen(RelationNode rel, int block_num);
extern int mdClose(int fd);
extern int mdRead(int fd, int block_num, void* buf, unsigned int size);
extern int mdWrite(int fd, int block_num, void* buf, unsigned int size);
extern int mdSeek(int fd, int offset);
extern int mdCount(RelationNode rel);	// return segment number in one relation
extern int mdCountPage(RelationNode rel);

#endif   /* SMGR_H */
