/* ---------------------------free space map---------------------------- */
extern int freespaceSearch(RelationNode rel, size_t tuple_size); // return the block_num of page
extern int freespaceUpdate(RelationNode rel, int block_num, size_t tuple_size);
