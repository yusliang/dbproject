#define DEBUG 0
/* ---------------------------buffer management---------------------------- */
#include "storage.h"

extern unsigned NBuffers;
extern unsigned NBufTables;

typedef struct
{
	BufferTag 	buf_tag;
	int 		buf_id;				// buffer location in shared buffer
	unsigned 	buf_flag;
	int			usage_count;	// usage counter for clock sweep code
	int 		next_freebuf;
//	unsigned	refcount;	// # of backends holding pins on buffer
} BufferDesc;

typedef struct
{
	int		next_victimbuffer;
	int		first_freebuffer;	// Head of list of unused buffers
	int		last_freebuffer;	// Tail of list of unused buffers
} BufferStrategyControl;

#define BUF_VALID 0x01
#define BUF_DIRTY 0x02

typedef struct BufferHashBucket
{
	BufferTag 			hash_key;
	int 				buf_id;
	struct BufferHashBucket 	*next_item;
} BufferHashBucket;

extern BufferDesc *buffer_descriptors;
extern char	   *buffer_blocks;
extern BufferStrategyControl *strategy_control;
extern BufferHashBucket *buffer_hashtable;

#define GetBufblockFromId(buf_id) ((void *) (buffer_blocks + ((unsigned) (buf_id)) * PAGE_SIZE))
#define GetBufHashBucket(hash_code) ((BufferHashBucket *) (buffer_hashtable + (unsigned) (hash_code)))

extern int readBuffer(BufferTag buf_tag, bool isExtend);
extern void invalidBuffer(BufferDesc *buf_hdr);
extern void flushBuffer(BufferDesc *buf_hdr);
extern void flushAllBuffers();
