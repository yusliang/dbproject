/*
 * b_plus_tree.c
 *
 *  Created on: Nov 2, 2014
 *      Author: tangshu
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "b_plus_tree.h"
#include "storage.h"
#include "bufmgr.h"

#define TablePageNum 10
#define NULL ((void*)0)
#define TRUE 1
#define REC_NUM 10000//datafile1_dd 10000
#define NODE_NUM 2600//datafile1_dd 2600
#define source_file "/home/tangshu/Downloads/src_org/datafile1_dd"



int Comp(const void *p1, const void *p2){
	Record *r1 = (Record *)p1;
	Record *r2 = (Record *)p2;
	if(r1->key <r2->key)
		return -1;
	if(r1->key == r2->key)
		return 0;
	return 1;
}
int up(int x1, int x2){
	if(x1%x2 == 0)
		return x1/x2;
	return x1/x2+1;
}
int min(int x1, int x2){
	if(x1 <x2)
		return x1;
	return x2;
}
Node node_list[NODE_NUM];
int node_list_num = 1;
int node_list_valid[NODE_NUM];
int writeNode(BufferTag bt, Node* node){
	/*int buf_id = readBuffer(bt);
	void *buf_block = GetBufblockFromId(buf_id);
	strncpy(buf_block, (char *) node, sizeof(Node));
	buffer_descriptors[buf_id].buf_flag |= BUF_DIRTY;*/
	node_list[bt.block_num] = *node;
	node_list_num++;
}
int setPageValid(){
	int i = 0;
	for(i = 0;i<node_list_num;i++)
		node_list_valid[i] = 1;
}
int resetParent(int i){
	if(node_list[i].type == LEAF)
		return 0;
	Node tmp = node_list[i];
	int j = 0;
	for(j = 0;j<tmp.count;j++){
		//printf("%d\n",tmp.pair[j].bt.block_num);
		node_list[tmp.pair[j].bt.block_num].parent = i;
		resetParent(tmp.pair[j].bt.block_num);
	}
}
int createBTree(RelationNode table, int KeyNo){	//the No.keyth attribute, assume its type as "int"
	printf("begin:");

	//get all records;
	Record * rcd_basic = malloc(sizeof(Record)*1);
	int rcd_basic_num = 0;
	int i = 0;
	/*for(i = 0;i<TablePageNum;i++){
		BufferTag buf_tag = {table,i};
		int buf_id = readBuffer(buf_tag);
		void *buf_block = GetBufblockFromId(buf_id);
		PageHeader *ph = (PageHeader *)buf_block;

		int j = 0;
		for(j = 0;ph->itemid[j].tuple_offset !=ph->next_tuple;j++){

			TupleHeader *tupHeader = (TupleHeader *)(buf_block+ph->itemid[j].tuple_offset);
			char *entry_attribute = (char *) (void *)(buf_block+ph->itemid[j].tuple_offset+sizeof(unsigned)+sizeof(unsigned)*(tupHeader->attribute_count));
			int k = 0;
			for (k = 0; k < KeyNo; k++)
				entry_attribute += tupHeader->attribute_length[k];
			int value = (unsigned)entry_attribute[tupHeader->attribute_length[k-1]];

			rcd = realloc(sizeof(Record),(rcd_num+1));
			rcd[rcd_num++] = (Record){value,{table,i},j};


		}
	}*/
	/*begin test*/
	rcd_basic = malloc(sizeof(Record)*REC_NUM);
	rcd_basic_num = 0;
	FILE* f= fopen(source_file, "r"); //打开文件;
	while(fscanf(f,"%d%d%d%d%d\n",&rcd_basic[rcd_basic_num++].key,&rcd_basic[rcd_basic_num].bt.rel.database,
			&rcd_basic[rcd_basic_num].bt.rel.relation,&rcd_basic[rcd_basic_num].bt.block_num,
			&rcd_basic[rcd_basic_num].ItemID)!=EOF){

		if(rcd_basic_num == REC_NUM)
			break;
	}
	fclose(f);
	 /*end test */
	//sort records
	qsort(rcd_basic, rcd_basic_num, sizeof(Record),Comp);
	//create tree AND store tree
	int rcd_layer_num = rcd_basic_num;
	Record * rcd_layer = rcd_basic;
	Record * rcd_uplayer = malloc(sizeof(Record)*(rcd_basic_num/MAX+1)) ;
	int rcd_uplayer_num = 0;

	RelationNode BIndexTable = {0,1};
	Node *node = (Node *)malloc(sizeof(Node));
	//initialize
	node->type = LEAF;
	node->count = 0;
	node->parent = up(rcd_basic_num, MAX)+1;

	int flag_specialdeal = -1;
	if(rcd_layer_num %MAX != 0 && rcd_layer_num %MAX <(MAX/2))
		flag_specialdeal = 1;
	else
		flag_specialdeal = -1;

	int pageNo = 1;//use page 0 to record the root
	while(rcd_layer_num !=1){
		int i = 0;
		for(i = 0;i<rcd_layer_num;i++){
			node->pair[node->count++] = rcd_layer[i];
			if(i !=0 && ((i+1) % MAX == 0 || i == rcd_layer_num-1))
			{
				//point the next node in the same layer
				if (rcd_layer_num == rcd_basic_num)
					node->type = LEAF;
				else
					node->type = NODE;
				if (rcd_uplayer_num == 0)
					node->parent = pageNo
							+ up(rcd_layer_num, MAX) + (i+1)/MAX/MAX;
				else
					node->parent = rcd_uplayer[0].bt.block_num
							+ up(rcd_layer_num, MAX) + (i+1)/MAX/MAX;
				if(rcd_layer_num == rcd_basic_num){
					if(i != rcd_layer_num-1){
						int nk = min(i+MAX-1, rcd_basic_num-1);
						node->pair[MAX+1] = (Record){rcd_layer[nk].key,{table,pageNo+1},-1};
					}
					else{
						Record tmp_r = {-1,{{0,0},-1},0};
						node->pair[MAX+1] = tmp_r;
					}
				}
				//store node
				BufferTag buf_tag = {BIndexTable,pageNo++};
				writeNode(buf_tag, node);


				rcd_uplayer[rcd_uplayer_num++] = (Record){rcd_layer[i].key,buf_tag,-1};

				//free(node);
				node = (Node *) malloc(sizeof(Node));
				//initialize

				node->count = 0;

				//try
				/*if(i == rcd_layer_num-1){
					int small = -1;
					if (rcd_layer_num == rcd_basic_num) {
						small = MAX / 2 + 1;
					} else
						small = MAX / 2;
					if(rcd_layer_num/MAX !=0 && rcd_layer_num%MAX <small){
						if(rcd_uplayer_num >=2)
							node_list[pageNo-3].pair[MAX+1].key = node_list[pageNo-2].pair[small-1].key;
						int k = 0;
						for (k = 0; k < node_list[pageNo - 1].count; k++) {
							node_list[pageNo - 1].pair[k + (MAX - small)] =
									node_list[pageNo - 1].pair[k];
						}
						for (k = 0; k < MAX - small; k++) {
							node_list[pageNo - 1].pair[k] =
									node_list[pageNo - 2].pair[k + small];
						}
						node_list[pageNo - 2].count = small;
						node_list[pageNo - 1].count += MAX - small;
						rcd_uplayer[rcd_uplayer_num - 2].key = node_list[pageNo
								- 2].pair[small - 1].key;
					}
				}*/
			}
		}
		if(flag_specialdeal == 1){
			int small = -1;
			if(rcd_layer_num == rcd_basic_num){
				small = MAX/2+1;
				if(rcd_uplayer_num >=2)
					node_list[pageNo-3].pair[MAX+1].key = node_list[pageNo-2].pair[small-1].key;
			}
			else
				small = MAX/2;
			int k = 0;
			for(k = 0;k<node_list[pageNo-1].count;k++){
				node_list[pageNo-1].pair[k+(MAX-small)] = node_list[pageNo-1].pair[k];
			}
			for(k = 0;k<MAX-small;k++){
				node_list[pageNo-1].pair[k] = node_list[pageNo-2].pair[k+small];
			}
			node_list[pageNo-2].count = small;
			node_list[pageNo-1].count += MAX - small;
			rcd_uplayer[rcd_uplayer_num-2].key = node_list[pageNo-2].pair[small-1].key;
		}

		rcd_layer = rcd_uplayer;
		rcd_layer_num = rcd_uplayer_num;
		//free(rcd_layer_tmpres);
		rcd_uplayer_num = 0;

		if (rcd_layer_num/MAX != 0 && rcd_layer_num % MAX!=0 &&  rcd_layer_num % MAX< (MAX / 2))
			flag_specialdeal = 1;
		else
			flag_specialdeal = -1;
	}
	node_list[0] = node_list[node_list_num-1];
	node_list[0].parent = -1;
	for(i = 0;i<node_list_num;i++)
	{
		if(node_list[i].parent == node_list_num-1)
			node_list[i].parent = 0;
	}
	node_list_num--;

	resetParent(0);
	setPageValid();
	return 0;
}


Node *readNode(BufferTag bt){
	/*int buf_id = readBuffer(bt);
	void *buf_block = GetBufblockFromId(buf_id);

	PageHeader_BIndex *ph_b = (PageHeader_BIndex *) buf_block;
	//read record
	Record page_rcd[ph_b->record_num];
	int i = 0;
	for (i = 0; i < ph_b->record_num; i++) {
		//TupleHeader *tupHeader = (TupleHeader *)(buf_block+ph->itemid[j].tuple_offset);
		Record * rr = (Record *) (buf_block + sizeof(ph_b->record_num)
				+ i * sizeof(Record));  //::
		page_rcd[i] = (Record) (*rr);
		//memcpy(page_rcd[i],rr);
	}
	Node* res_node = malloc(sizeof(Node));
	res_node->count = ph_b->count;
	//res_node->pair = page_rcd;
	int j = 0;
	for (j = 0; j < res_node->count; j++)
		res_node->pair[j] = page_rcd[j];
	res_node->parent = ph_b->parent;
	res_node->type = ph_b->type;*/
	Node* res_node;
	res_node = &node_list[bt.block_num];
	qsort(res_node->pair, res_node->count, sizeof(Record), Comp);
	return res_node;
}

BufferTag searchBTree_Node(RelationNode BIndexTable, int KeyValue){

		BufferTag buf_tag = {BIndexTable,0};
		while (1) {
			Node * page_node = readNode(buf_tag);
			if(page_node->type == LEAF)
				return buf_tag;
			//sort record
			qsort(page_node->pair, page_node->count, sizeof(Record), Comp);
			//BinarySearch
			int bb = 0;
			int ee = page_node->count - 1;
			int mid = -1;
			while (bb < ee) {
				mid = (bb + ee) / 2;
				if (page_node->pair[mid].key == KeyValue ||
						(mid + 1 <page_node->count && page_node->pair[mid+1].key == KeyValue)) {
					break;

				}
				if((bb + ee) / 2 == bb)
					break;

				if (page_node->pair[mid].key < KeyValue) {
					bb = mid;
				} else
					ee = mid;
			}
			if (bb < ee && (bb + ee) / 2 != bb)  //find
			{
				if(page_node->pair[mid].key == KeyValue)
					buf_tag = page_node->pair[mid].bt;
				else{
					buf_tag = page_node->pair[mid+1].bt;}
			}
			else {  // do not find
				if (KeyValue <= page_node->pair[mid].key)
					buf_tag = page_node->pair[mid].bt;  //res = &(page_rcd[mid-1]);
				else
					buf_tag = page_node->pair[mid+1].bt; //res = &(page_rcd[mid]);
			}
		}
}
int searchBTree_Record(RelationNode BIndexTable, int KeyValue, Record * res){
	BufferTag res_buffer = searchBTree_Node(BIndexTable, KeyValue);
	Node * res_node = readNode(res_buffer);

	//BinarySearch
	int bb = 0;
	int ee = res_node->count - 1;
	int mid = -1;
	while (bb < ee) {
		mid = (bb + ee) / 2;
		if (res_node->pair[mid].key == KeyValue||
				(mid + 1 <res_node->count && res_node->pair[mid+1].key == KeyValue)) {
			break;

		}

		if (res_node->pair[mid].key < KeyValue) {
			bb = mid;
		} else
			ee = mid;
	}
	if (bb < ee)  //find
	{

		if(res_node->pair[mid].key == KeyValue){
			res->key = res_node->pair[mid].key;
			res->bt.block_num = res_node->pair[mid].bt.block_num;
			res->bt.rel.database = res_node->pair[mid].bt.rel.database;
			res->bt.rel.relation = res_node->pair[mid].bt.rel.relation;
			res->ItemID = res_node->pair[mid].ItemID;
			//res = &(res_node->pair[mid]);
		}
		else{
			res->key = res_node->pair[mid+1].key;
			res->bt.block_num = res_node->pair[mid+1].bt.block_num;
			res->bt.rel.database = res_node->pair[mid+1].bt.rel.database;
			res->bt.rel.relation = res_node->pair[mid+1].bt.rel.relation;
			res->ItemID = res_node->pair[mid+1].ItemID;
			//res = &(res_node->pair[mid+1]);
		}
		return 1;
	}
	return -1;
}





//table represents the BIndex
int getKeyIndexFromNode(Node *node, int KeyValue){
	//binary search
	int bb = 0;
	int ee = node->count - 1;
	int mid = -1;
	while (bb < ee) {
		mid = (bb + ee) / 2;
		if (node->pair[mid].key == KeyValue) {
			return mid;
		}

		if (node->pair[mid].key < KeyValue) {
			bb = mid;
		} else
			ee = mid;
	}

	return bb+1;
}
int MaxRec(Record rr[],int count){
	int i = 0;
	Record res = rr[0];
	int resInd = 0;
	for(i = 1;i<count;i++){
		if(rr[i].key>res.key){
			res = rr[i];
			resInd = i;
		}
	}
	return i;
}
int updateParentKey(BufferTag bt_node, Node node){
	BufferTag bt_par = { bt_node.rel, node.parent };
	Node* node_par = readNode(bt_par);
	int i = 0;
	for (i = 0; i < node_par->count; i++) {
		if (node_par->pair[i].bt.block_num == bt_node.block_num)
			break;
	}
	int max = MaxRec(node.pair, node.count);
	if (node_par->pair[i].key < node.pair[max].key)
		node_par->pair[i].key = node.pair[max].key;
	return 1;
}
int findParKey(Node node){
	Node node_par = node_list[node.parent];
	int i = 0;
	for (i = 0; i < node_par.count; i++) {
		if (node_par.pair[i].key >= node.pair[node.count-1].key)
			return i;
	}
	return -1;
}
int updateRecArray(Record r1[], Record r2[], int r2_len){
	int i = 0;
	for(i = 0;i<r2_len;i++)
		r1[i] = r2[i];

}
int getPageNum(RelationNode table){
	int page_num =1;
	return page_num;
}
int insertBTree_node(RelationNode BIndexTable, BufferTag bt_node, Node* node,Record rr ){
	//insert the rr into search_node
		if(node->count < MAX){
			node->pair[node->count++] = rr;
			updateParentKey(bt_node, *node);
			writeNode(bt_node,node);
			return 1;
		}
		//leaf over-flow
		//find next sibling

		BufferTag bt_node_next = node->pair[MAX+1].bt;
		Node* next_node = readNode(bt_node_next);
		if(next_node->count <MAX){
			Record tmp_rr[node->count+next_node->count+1];
			int i = 0;
			for(i = 0;i<node->count;i++){
				tmp_rr[i] = node->pair[i];
			}
			for(i = node->count;i<node->count+next_node->count;i++){
				tmp_rr[i] = next_node->pair[i-node->count];
			}
			tmp_rr[node->count+next_node->count] = rr;
			qsort(tmp_rr, node->count+next_node->count+1, sizeof(Record),Comp);


			for (i = 0; i < MAX; i++) {
				node->pair[i] = tmp_rr[i];
			}
			updateParentKey(bt_node, *node);
			for(i = MAX; i<node->count+next_node->count+1; i++){
				next_node->pair[i-MAX] = tmp_rr[i];
			}
			next_node->count++;


			writeNode(bt_node,node);
			writeNode(bt_node_next,next_node);


		}
		else{	//both this node and the next node already full
			int n1 = (MAX+1)/2;
			int n2 = MAX + 1 - n1;
			Record tmp_rr1[n1];
			Record tmp_rr2[n2];

			Record tmp_rr[MAX + 1];
			int i = 0;
			for (i = 0; i < MAX; i++) {
				tmp_rr[i] = node->pair[i];
			}
			tmp_rr[MAX] = rr;
			qsort(tmp_rr, MAX+1, sizeof(Record),Comp);

			for(i = 0;i < n1; i++){
				tmp_rr1[i] = tmp_rr[i];
			}
			for(i = n1;i<n2;i++){
				tmp_rr2[i-n1] = tmp_rr[i];
			}
			updateRecArray(node->pair, tmp_rr1, n1);
			next_node->count = n1;
			updateParentKey(bt_node, *node);


			Node* added_node = malloc(sizeof(Node));
			added_node->count = n2;
			added_node->type = 1;
			added_node->parent = node->parent;
			updateRecArray(added_node->pair, tmp_rr2, n2);

			int page_num = getPageNum(BIndexTable);
			BufferTag added_bt = {bt_node.rel, page_num++ };

			Record rtmp = {-1, added_bt, -1};
			node->pair[MAX+1] = rtmp;
			Record rtmp2 = {-1, bt_node_next, -1};
			added_node->pair[MAX+1] = rtmp2;

			writeNode(added_bt,added_node);
			writeNode(bt_node, node);

			Record new_rr = {tmp_rr1[n1-1].key, added_bt, -1};
			if(node->parent != -1){
				BufferTag bt_par = { bt_node.rel, node->parent };
				Node* node_par = readNode(bt_par);
				insertBTree_node(BIndexTable, bt_par, node_par, new_rr);
			}
			else{
				//add root
			}
			return 0;
		}
		return 0;
}
int insertBTree(RelationNode BIndexTable, Record rr){	//the key in Record represents KeyValue
	//search
	BufferTag bt_node = searchBTree_Node(BIndexTable, rr.key);
	Node* node = readNode(bt_node);
	insertBTree_node(BIndexTable, bt_node, node, rr);

	return 0;
}

int findNeighbors(Node node, int direction, int k,BufferTag* res_bt){
	if(node.type == LEAF && direction == 1)
	{
		if(node.pair[MAX+1].key !=-1){
			*res_bt = node.pair[MAX+1].bt;
			return 1;
		}
		return -1;
	}
	Node tmp = node;
	int layer = 0;
	int found = -1 ;
	int tmp_k = k;
	while(tmp.parent != -1){
		tmp = node_list[tmp.parent];
		if(direction == 1){
			if(tmp_k == tmp.count-1){
				layer++;
				tmp_k = findParKey(tmp);
			}
			else{
				found = 1;
				break;
			}
		}
		if (direction == -1) {
			if (tmp_k == 0) {
				layer++;
				tmp_k = findParKey(tmp);
			} else{
				found = 1;
				break;
			}
		}
	}
	if(found == -1)
		return -1;
	Node *res;
	if(direction == 1){
		res = readNode(tmp.pair[tmp_k+1].bt);
		while((layer--)!=0)
			res = readNode(res->pair[0].bt);
	}
	if (direction == -1) {
		res = readNode(tmp.pair[tmp_k-1].bt);
		while ((layer--) != 0)
			res = readNode(res->pair[res->count-1].bt);
	}
	int k_p = findParKey(*res);
	*res_bt = node_list[res->parent].pair[k_p].bt;//!!!!
	return 1;
}
int updateKey(Node *nn){
	if(nn->type == LEAF)
		return 1;
	int i = 0;
	for(i =0;i<nn->count;i++){
		updateKey(&node_list[nn->pair[i].bt.block_num]);
		Node *tmp_c = &node_list[nn->pair[i].bt.block_num];
		nn->pair[i].key = tmp_c->pair[tmp_c->count-1].key;
	}
	return 0;
}
int deleteBTree_node(RelationNode BIndexTable, Node* node, Record rr){
	int small;
	if(node->type == LEAF)
		small = (MAX/2)+1;
	else
		small = MAX/2;
	if(node->parent == -1)
		small = 1;

	//the last record
	if(node->count == 1 ){
		node_list_valid[0]=-1;
		return 1;
	}

	if(node->count - 1 >= small){
		Node * tmp = node;
		int pp_kk[20];	//20 represents the tree's depth
		int pp_kk_num = 0;
		while (tmp->parent != -1) {
			pp_kk[pp_kk_num++] = findParKey(*tmp);
			tmp = &node_list[tmp->parent];
		}

		int ii = findParKey(*node);

		Record tmp_rr[node->count-1];
		int i = 0;int cc = 0;
		for(i = 0;i<node->count;i++){
			if(node->pair[i].key != rr.key)
				tmp_rr[cc++] = node->pair[i];
		}
		for(i = 0;i<cc;i++){
			node->pair[i] = tmp_rr[i];
		}
		node->count--;
		qsort(node->pair, node->count, sizeof(Record),Comp);


		updateKey(node);
		int kkk = 0;
		Node *parpar = node;
		for (kkk = 0; kkk < pp_kk_num; kkk++) {
			parpar = &node_list[parpar->parent];
			int tt = pp_kk[kkk];
			Node *cc = &node_list[parpar->pair[tt].bt.block_num];
			//if (parpar->pair[tt].key < cc->pair[cc->count - 1].key)
				parpar->pair[tt].key = cc->pair[cc->count - 1].key;
		}
	}
	else{
		//get next node
		int ii = findParKey(*node);

		BufferTag* neighbor_bt = malloc(sizeof(BufferTag));
		int direction = 1;
		int ret = findNeighbors(*node, direction,ii, neighbor_bt);
		if(ret == -1){
			direction = -1;
			ret = findNeighbors(*node, direction, ii,neighbor_bt);
		}
		Node* next_node = &node_list[neighbor_bt->block_num];
		if(direction == -1)
		{
			if(ret == -1){
				//delete root, set this node as root
				int k_p = findParKey(*node);
				int bt_no = node_list[node->parent].pair[k_p].bt.block_num;
				node_list[0] = node_list[bt_no];
				node_list[0].parent = -1;
				resetParent(0);
				node_list_valid[bt_no] = -1;

				deleteBTree_node(BIndexTable,&node_list[0], rr);

				return 0;
			}

			Node* tmp = next_node;
			next_node = node;
			node = tmp;
		}
		if((node->count + next_node->count-1) >= 2*small){ //no need to delete
			Node * tmp = node;
			int pp_kk[20];//20 represents the tree's depth
			int pp_kk_num = 0;
			while(tmp->parent != -1){
				pp_kk[pp_kk_num++] = findParKey(*tmp);
				tmp = &node_list[tmp->parent];
			}


			//int small = MAX/2+1;
			Record tmp_rr[node->count +next_node->count- 1];
			int i = 0;
			int cc = 0;
			for (i = 0; i < node->count; i++) {
				if (node->pair[i].key != rr.key)
					tmp_rr[cc++] = node->pair[i];
			}
			for (i = 0; i < next_node->count; i++) {
				if (next_node->pair[i].key != rr.key)
					tmp_rr[cc++] = next_node->pair[i];
			}
			for (i = 0; i < small; i++) {
				node->pair[i] = tmp_rr[i];
			}
			for (i = small; i < node->count+next_node->count-1; i++) {
				next_node->pair[i-small] = tmp_rr[i];
			}
			qsort(node->pair, node->count, sizeof(Record), Comp);

			next_node->count = node->count + next_node->count -1-small;
			node->count = small;


			updateKey(node);updateKey(next_node);
			int kkk = 0;
			Node *parpar = node;
			for (kkk = 0; kkk < pp_kk_num; kkk++) {
				parpar = &node_list[parpar->parent];
				int tt = pp_kk[kkk];
				Node *cc = &node_list[parpar->pair[tt].bt.block_num];
				//if (parpar->pair[tt].key < cc->pair[cc->count - 1].key)
					parpar->pair[tt].key = cc->pair[cc->count - 1].key;
			}
			//updateParentKey(bt_node, node);
		}
		else{ //delete this node's next node
			//change this node
			Node * tmp = node;
			int pp_kk[20];			//20 represents the tree's depth
			int pp_kk_num = 0;
			while (tmp->parent != -1) {
				pp_kk[pp_kk_num++] = findParKey(*tmp);
				tmp = &node_list[tmp->parent];
			}



			int k = findParKey(*node);
			Record tmp_rr[node->count + next_node->count - 1];
			int i = 0;
			int cc = 0;
			for (i = 0; i < node->count; i++) {
				if (node->pair[i].key != rr.key)
					tmp_rr[cc++] = node->pair[i];
			}
			for (i = 0; i < next_node->count; i++) {
				if (next_node->pair[i].key != rr.key)
					tmp_rr[cc++] = next_node->pair[i];
			}
			for (i = 0; i < cc; i++) {
				node->pair[i] = tmp_rr[i];
			}

			if(node->type ==1)
				node->pair[MAX+1] = next_node->pair[MAX+1];
			node->count = node->count+next_node->count -1;


			//delete next node
			int k_next = findParKey(*next_node);
			Record delete_rec = node_list[next_node->parent].pair[k_next];
			node_list_valid[node_list[next_node->parent].pair[k_next].bt.block_num] = -1;
			if(next_node->parent == -1)
				return 0;

			deleteBTree_node(BIndexTable, &node_list[next_node->parent], delete_rec);
			int kkk = 0;
			Node *parpar = node;
			for (kkk = 0; kkk < pp_kk_num; kkk++) {
				parpar = &node_list[parpar->parent];
				int tt = pp_kk[kkk];
				Node *cc = &node_list[parpar->pair[tt].bt.block_num];
				//if (parpar->pair[tt].key < cc->pair[cc->count - 1].key)
				parpar->pair[tt].key = cc->pair[cc->count - 1].key;
			}
			return 0;
		}
	}
	return 0;
}
int deleteBTree(RelationNode BIndexTable, Record rr){
	BufferTag bt_node = searchBTree_Node(BIndexTable, rr.key);
	Node* node = readNode(bt_node);
	deleteBTree_node(BIndexTable, node, rr);//delete rr from node


	resetParent(0);
	return 0;
}
int writeFile(Node node,FILE *f,int bid){
	/*
	 * <node type="" count="" record="|---;|---" parent = "">
	<children>

	</children>
</node>
*/
	fprintf( f, "%s%d%s%d%s%d%s", "<node bid=\"",bid, "\" type=\"", node.type,
			"\" count=\"",node.count, "\" record=\"" );
	int i = 0;
	for(i = 0;i<node.count;i++){
		fprintf(f, "%d%s%d%s%d%s%d%s%d%s", node.pair[i].key, "|",node.pair[i].bt.rel.database, ",",node.pair[i].bt.rel.relation, ",",node.pair[i].bt.block_num, ",",node.pair[i].ItemID,";");
	}
	fprintf(f,"%s%d%s","\" parent=\"",node.parent,"\"");
	if(node.type == LEAF){
		fprintf(f, "%s%d%s", " nextid=\"",node.pair[6].bt.block_num,"\"");
	}
	fprintf(f,">");
	if(node.type != LEAF){
		fprintf(f,"\n<children>\n");
		for(i = 0;i<node.count;i++){
			if(node_list_valid[node.pair[i].bt.block_num] !=-1)
				writeFile(node_list[node.pair[i].bt.block_num],f, node.pair[i].bt.block_num);
		}
		fprintf(f,"\n</children>");
	}
	fprintf(f,"\n</node>\n");
	return 0;
}
int writeTree(char * filepath){
	FILE* f= fopen(filepath, "w+"); //打开文件;
	if (node_list_valid[0] != -1) {
		Node root = node_list[0];
		writeFile(root, f, 0);
	}
	fclose(f);
}
//int main(int argc, char *argv[]){
//	RelationNode rn = {0,0};
//	createBTree(rn,1);
//	writeTree("/home/tangshu/Downloads/src_org/btree.xml");
//
//	FILE *f = fopen("/home/tangshu/Downloads/src_org/datafile2", "r");;
//		int search_key = -1;int i = 0;
//		for (i = 0; i <5000; i++) {
//			fscanf(f,"%d\n",&search_key);
//			RelationNode bindex = { 0, 1 };
//			Record * res = malloc(sizeof(Node));
//			int ss = searchBTree_Record(bindex, search_key, res);
//			//printf("\n%d%d", res->bt.block_num, res->ItemID);
//
//			deleteBTree(bindex, *res);
//			printf("%s%d","\ncomplete:",i);
//		}
//		fclose(f);
//		f = fopen("/home/tangshu/Downloads/src_org/datafile3", "r");
//			for (i = 0; i <4990; i++) {
//				fscanf(f,"%d\n",&search_key);
//				RelationNode bindex = { 0, 1 };
//				Record * res = malloc(sizeof(Node));
//				int ss = searchBTree_Record(bindex, search_key, res);
//				//printf("\n%d%d", res->bt.block_num, res->ItemID);
//
//				deleteBTree(bindex, *res);
//				printf("%s%d","\ncomplete:",i);
//			}
//			fclose(f);
//	/*int i = 0;//FILE *f = fopen("/home/tangshu/Downloads/src_org/datafile2", "r");;
//	int search_key = -1;
//	for (i = 50; i >=0; i--) {
//		//fscanf(f,"%d\n",search_key);
//		if(i == 42)
//			printf("xx");
//		RelationNode bindex = { 0, 1 };
//		Record * res = malloc(sizeof(Node));
//		int ss = searchBTree_Record(bindex, i, res);
//		//printf("\n%d%d", res->bt.block_num, res->ItemID);
//
//		deleteBTree(bindex, *res);
//		printf("%s%d","\ncomplete:",i);
//	}*/
//
//	writeTree("/home/tangshu/Downloads/src_org/btree_afterdelete.xml");
//	printf("\nend");
//	//fclose(f);
//	return 0;
//}
