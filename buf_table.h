#ifndef BUFTABLE_H
#define BUFTABLE_H

extern void initBufTable(int size);
extern unsigned buftableHashcode(BufferTag *buf_tag);
extern int buftableLookup(BufferTag *buf_tag, unsigned hash_code);
extern int buftableInsert(BufferTag *buf_tag, unsigned hash_code, int buf_id);
extern int buftableDelete(BufferTag *buf_tag, unsigned hash_code);
#endif   /* BUFTABLE_H */
