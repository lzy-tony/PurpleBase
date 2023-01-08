#include <cstring>

#include "ix.h"

IX_IndexHandle::IX_IndexHandle(FileManager *_fm, BufPageManager *_bpm, int _fid) {
    fm = _fm, bpm = _bpm, fid = _fid;
    int index;
    BufType header_buf = bpm -> getPage(fid, 0, index);
    memcpy(&header, header_buf, sizeof(IX_FileHeader));
}

BLinkNode* IX_IndexHandle::makeNode(int pid) {
    if (pid == 0)
        return NULL;
    BLinkNode *node = new BLinkNode;
    int index;
    BufType page_buf = bpm -> getPage(fid, pid, index);
    bpm -> access(index);

    memcpy(&node -> meta, page_buf, sizeof(IX_PageHeader));
    node -> pages = page_buf + header.pages_offset;
    node -> slots = page_buf + header.slots_offset;
    node -> children = page_buf + header.children_offset;
    node -> keys = (unsigned char *) (page_buf + header.keys_offset);
    node -> index = index;
    node -> data = page_buf;
    return node;
}

bool IX_IndexHandle::LT(void *iData1, void *iData2,
                        int p1, int p2, 
                        int s1, int s2, 
                        AttrType attrType, int attrLength) {
    // returns iData1 < iData2
    if (attrType == INT_ATTRTYPE) {
        if (*((int *) iData1) != *((int *) iData2)) {
            return *((int *) iData1) < *((int *) iData2);
        } else if (p1 != p2) {
            return p1 < p2;
        } else {
            return s1 < s2;
        }
    } else if (attrType == FLOAT_ATTRTYPE) {
        if (*((float *) iData1) != *((float *) iData2)) {
            return *((float *) iData1) < *((float *) iData2);
        } else if (p1 != p2) {
            return p1 < p2;
        } else {
            return s1 < s2;
        }
    } else {
        int cmp = std::memcmp((char *) iData1, (char *) iData2, attrLength);
        if (cmp != 0) {
            return cmp < 0;
        } else if (p1 != p2) {
            return p1 < p2;
        } else {
            return s1 < s2;
        }
    }
}

bool IX_IndexHandle::EQ(void *iData1, void *iData2,
                        int p1, int p2, 
                        int s1, int s2, 
                        AttrType attrType, int attrLength) {
    if (p1 != p2 || s1 != s2) {
        return false;
    }
    if (attrType == INT_ATTRTYPE) {
        return *((int *) iData1) == *((int *) iData2);
    } else if (attrType == FLOAT_ATTRTYPE) {
        return *((float *) iData1) == *((float *) iData2);
    } else {
        return (std::memcmp((char *) iData1, (char *) iData2, attrLength) == 0);
    }
}

int IX_IndexHandle::rank(BLinkNode *node, void *indexData, int pid, int sid) {
    /*
        returns rank [0, num]
    */
    for (int i = node -> meta.num - 1; i >= 0; i--) {
        if (LT(indexData, (void *) (node -> keys + i * header.attrLength),
            pid, node -> pages[i],
            sid, node -> slots[i],
            header.attrType, header.attrLength))
            continue;
        return i + 1;
    }
    return 0;
}

std::pair<BLinkNode*, int> IX_IndexHandle::find(int id, void *indexData, int pid, int sid) {
    BLinkNode *node = makeNode(id);
    if (node == NULL)
        return std::make_pair(node, -1);
    if (node -> meta.is_leaf) {
        return std::make_pair(node, rank(node, indexData, pid, sid));
    } else {
        int r = rank(node, indexData, pid, sid);
        int next_id = node -> children[r];
        delete node;
        return find(next_id, indexData, pid, sid);
    }
}

BLinkNode* IX_IndexHandle::findMin(int id) {
    BLinkNode *node = makeNode(id);
    if (node -> meta.is_leaf) {
        return node;
    }
    int next_id = node -> children[0];
    delete node;
    return findMin(next_id);
}

bool IX_IndexHandle::writeHeader(BLinkNode *node) {
    memcpy(node -> data, &(node -> meta), sizeof(IX_PageHeader));
    bpm -> markDirty(node -> index);
    return true;
}

void IX_IndexHandle::splitUp(BLinkNode *node) {
    /*
        for a (3, 5) tree
        leaf: 
                          1
        0 0 1 2 2 => 0 0     1 2 2

        non-leaf: 
                          1
        0 0 1 2 2 => 0 0     2 2
    */
    if (node -> meta.num + 1 <= header.maxNum) {
        // delete node
        delete node;
        return;
    }
    int index;
    // create new page
    int new_id = header.totalPage++;
    BufType new_page = bpm -> getPage(fid, new_id, index);
    memset(new_page, 0, PAGE_SIZE);
    bpm -> access(index);
    bpm -> markDirty(index);

    // split
    // leaf: 
    // [0, maxNum - 1] -> [0, minNum - 2] (minNum - 1) [minNum - 1, maxNum - 1]
    // non-leaf:
    // [0, maxNum - 1] -> [0, minNum - 2] minNum - 1 [minNum, maxNum - 1]
    // right
    BLinkNode *rnode = makeNode(new_id);
    int mid = header.minNum - 1;
    rnode -> data = new_page;
    rnode -> index = index;
    if (node -> meta.is_leaf) {
        memcpy(rnode -> pages, node -> pages + mid, (header.maxNum - header.minNum + 1) << 2);
        memcpy(rnode -> slots, node -> pages + mid, (header.maxNum - header.minNum + 1) << 2);
        memcpy(rnode -> keys, node -> keys + mid * header.attrLength, (header.maxNum - header.minNum + 1) * header.attrLength);
        memcpy(rnode -> children, node -> children + mid, (header.maxNum - header.minNum + 2) << 2);
        rnode -> meta.num = (header.maxNum - header.minNum + 1);
    } else {
        memcpy(rnode -> pages, node -> pages + mid + 1, (header.maxNum - header.minNum) << 2);
        memcpy(rnode -> slots, node -> pages + mid + 1, (header.maxNum - header.minNum) << 2);
        memcpy(rnode -> keys, node -> keys + (mid + 1) * header.attrLength, (header.maxNum - header.minNum) * header.attrLength);
        memcpy(rnode -> children, node -> children + mid + 1, (header.maxNum - header.minNum + 1) << 2);
        rnode -> meta.num = (header.maxNum - header.minNum);
    }
    
    rnode -> meta.is_leaf = node -> meta.is_leaf;
    rnode -> meta.id = new_id;
    rnode -> meta.prv = node -> meta.id;
    rnode -> meta.nxt = node -> meta.nxt;
    rnode -> meta.father = (node -> meta.father == 0) ? header.totalPage : node -> meta.father;

    writeHeader(rnode);

    if (rnode -> meta.nxt) {
        BLinkNode *nnxt = makeNode(rnode -> meta.nxt);
        nnxt -> meta.prv = new_id;
        writeHeader(nnxt);
        delete nnxt;
    }

    if (!rnode -> meta.is_leaf) {
        for (int i = 0; i < rnode -> meta.num + 1; i++) {
            BLinkNode *c = makeNode(rnode -> children[i]);
            c -> meta.father = rnode -> meta.id;
            writeHeader(c);
            delete c;
        }
    }

    // mid and up
    BLinkNode *father;
    unsigned char *mid_index = node -> keys + mid * header.attrLength;
    if (node -> meta.father == 0) {
        // already root, need to build new root
        int rt_id = header.totalPage++;
        int rt_index;
        BufType rt_page = bpm -> getPage(fid, rt_id, rt_index);
        memset(rt_page, 0, PAGE_SIZE);

        father = makeNode(rt_id);
        father -> meta.is_leaf = 0;
        father -> meta.id = rt_id;
        father -> meta.prv = 0;
        father -> meta.nxt = 0;
        father -> meta.father = 0;
        father -> meta.num = 1;
        father -> data = rt_page;
        father -> pages[0] = node -> pages[mid];
        father -> slots[0] = node -> slots[mid];
        father -> children[0] = node -> meta.id;
        father -> children[1] = rnode -> meta.id;
        memcpy(father -> keys, mid_index, header.attrLength);
        father -> index = rt_index;

        writeHeader(father);

        bpm -> access(rt_index);
        bpm -> markDirty(rt_index);

        header.root = rt_id;
        node -> meta.father = rt_id;

        // write back header
        int header_index;
		BufType header_buf = bpm -> getPage(fid, 0, header_index);
		bpm -> access(header_index);
		memcpy(header_buf, &header, sizeof(IX_FileHeader));
		bpm -> markDirty(header_index);
    } else {
        father = makeNode(node -> meta.father);
        int mid_rank = rank(father, mid_index, node -> pages[mid], node -> slots[mid]);
        for (int i = father -> meta.num - 1; i >= mid_rank; i--) {
            // copy i to i + 1
            father -> pages[i + 1] = father -> pages[i];
            father -> slots[i + 1] = father -> slots[i];
            father -> children[i + 2] = father -> children[i + 1];
            memcpy(father -> keys + (i + 1) * header.attrLength, father -> keys + i * header.attrLength, header.attrLength);
        }
        father -> meta.num++;
        father -> pages[mid_rank] = node -> pages[mid];
        father -> slots[mid_rank] = node -> slots[mid];
        father -> children[mid_rank + 1] = new_id;
        memcpy(father -> keys + mid_rank * header.attrLength, mid_index, header.attrLength);

        writeHeader(father);
    }

    // left
    memset(node -> pages + mid, 0, (header.maxNum - header.minNum + 1) << 2);
    memset(node -> slots + mid, 0, (header.maxNum - header.minNum + 1) << 2);
    memset(node -> keys + mid * header.attrLength, 0, (header.maxNum - header.minNum + 1) * header.attrLength);
    memset(node -> children + mid + 1, 0, (header.maxNum - header.minNum + 1) << 2);
    node -> meta.nxt = new_id;
    node -> meta.num = mid;

    writeHeader(node);

    delete node;
    delete rnode;
    splitUp(father);
}

void IX_IndexHandle::updateLeftUp(int id, void *newData, void *oldData,
                                  int newp, int oldp,
                                  int news, int olds) {
    if (id == 0) {
        return;
    }
    BLinkNode *node = makeNode(id);
    int r = rank(node, oldData, oldp, olds) - 1;
    if (EQ(oldData, node -> keys + r * header.attrLength,
           oldp, node -> pages[r],
           olds, node -> slots[r],
           header.attrType, header.attrLength)) {
        node -> pages[r] = newp;
        node -> slots[r] = news;
        memcpy(node -> keys + r * header.attrLength, newData, header.attrLength);
        bpm -> markDirty(node -> index);
        delete node;
    } else {
        int father_id = node -> meta.father;
        delete node;
        updateLeftUp(father_id, newData, oldData, newp, oldp, news, olds);
    }
}

void IX_IndexHandle::print(int id) {
    BLinkNode *node = makeNode(id);
    std::cerr << std::endl << std::endl;
    std::cerr << "is_leaf: " << node -> meta.is_leaf << std::endl;
    std::cerr << "prv: " << node -> meta.prv << " id: " << node -> meta.id << " nxt: " << node -> meta.nxt << std::endl;
    std::cerr << "father: " << node -> meta.father << std::endl;
    std::cerr << "key num: " << node -> meta.num << std::endl;

    std::cerr << "keys ";
    for (int i = 0; i < node -> meta.num; i++) {
        std::cerr << *((float *) (node -> keys + i * header.attrLength)) << " ";
    }
    std::cerr << std::endl;
    std::cerr << "pages ";
    for (int i = 0; i < node -> meta.num; i++) {
        std::cerr << ((int *) node -> pages)[i] << " ";
    }
    std::cerr << std::endl;
    std::cerr << "slots ";
    for (int i = 0; i < node -> meta.num; i++) {
        std::cerr << ((int *) node -> slots)[i] << " ";
    }
    std::cerr << std::endl;

    std::cerr << "children ";
    for (int i = 0; i < node -> meta.num + 1; i++) {
        std::cerr << ((int *) node -> children)[i] << " ";
    }
    std::cerr << std::endl;


    if (node -> meta.is_leaf || node -> meta.num == 0) {
        delete node;
        return;
    } else {
        for (int i = 0; i < node -> meta.num + 1; i++) {
            print(node -> children[i]);            
        }
        delete node;
    }
}

void IX_IndexHandle::mergeUp(BLinkNode *node) {
    if (node -> meta.father == 0) { // top
        delete node;
        return;
    }
    if (node -> meta.num + 1 >= header.minNum) {
        delete node;
        return;
    }
    BLinkNode *father = makeNode(node -> meta.father);
    
    BLinkNode *prv, *nxt;
    if (node -> meta.prv)
        prv = makeNode(node -> meta.prv);
    if (node -> meta.nxt)
        nxt = makeNode(node -> meta.nxt);
    if (node -> meta.prv && node -> meta.father == prv -> meta.father && prv -> meta.num + 1 >= header.minNum + 1) {
        // borrow from previous sibling
        if (node -> meta.is_leaf) {
            /*
                1. update father
                2. move current
                3. update sibling, clean tail
            */
            for (int i = 0; i < father -> meta.num; i++) {
                if (EQ(node -> keys, father -> keys + i * header.attrLength,
                       node -> pages[0], father -> pages[i],
                       node -> slots[0], father -> slots[i],
                       header.attrType, header.attrLength)) {
                    father -> pages[i] = prv -> pages[prv -> meta.num - 1];
                    father -> slots[i] = prv -> slots[prv -> meta.num - 1];
                    memcpy(father -> keys + i * header.attrLength, prv -> keys + (prv -> meta.num - 1) * header.attrLength, header.attrLength);

                    bpm -> markDirty(father -> index);
                    break;
                }
            }
            
            for (int i = node -> meta.num - 1; i >= 0; i--) {
                // copy i to i + 1
                node -> pages[i + 1] = node -> pages[i];
                node -> slots[i + 1] = node -> slots[i];
                node -> children[i + 2] = node -> children[i + 1];
                memcpy(node -> keys + (i + 1) * header.attrLength, node -> keys + i * header.attrLength, header.attrLength);
            }
            node -> pages[0] = prv -> pages[prv -> meta.num - 1];
            node -> slots[0] = prv -> slots[prv -> meta.num - 1];
            memcpy(node -> keys, prv -> keys + (prv -> meta.num - 1) * header.attrLength, header.attrLength);
            node -> meta.num++;
            writeHeader(node);

            prv -> pages[prv -> meta.num - 1] = 0;
            prv -> slots[prv -> meta.num - 1] = 0;
            memset(prv -> keys + (prv -> meta.num - 1) * header.attrLength, 0, header.attrLength);
            prv -> meta.num--;
            writeHeader(prv);
        } else {
            int r = rank(father, node -> keys, node -> pages[0], node -> slots[0]) - 1;

            for (int i = node -> meta.num - 1; i >= 0; i--) {
                // copy i to i + 1
                node -> pages[i + 1] = node -> pages[i];
                node -> slots[i + 1] = node -> slots[i];
                node -> children[i + 2] = node -> children[i + 1];
                memcpy(node -> keys + (i + 1) * header.attrLength, node -> keys + i * header.attrLength, header.attrLength);
            }
            node -> children[1] = node -> children[0];

            node -> pages[0] = father -> pages[r];
            node -> slots[0] = father -> slots[r];
            memcpy(node -> keys, father -> keys + r * header.attrLength, header.attrLength);
            node -> children[0] = prv -> children[prv -> meta.num];
            node -> meta.num++;
            writeHeader(node);

            BLinkNode *c = makeNode(node -> children[0]);
            c -> meta.father = node -> meta.id;
            writeHeader(c);
            delete c;

            father -> pages[r] = prv -> pages[prv -> meta.num - 1];
            father -> slots[r] = prv -> slots[prv -> meta.num - 1];
            memcpy(father -> keys + r * header.attrLength, prv -> keys + (prv -> meta.num - 1) * header.attrLength, header.attrLength);
            bpm -> markDirty(father -> index);

            prv -> pages[prv -> meta.num - 1] = 0;
            prv -> slots[prv -> meta.num - 1] = 0;
            prv -> children[prv -> meta.num] = 0;
            memset(prv -> keys + (prv -> meta.num - 1) * header.attrLength, 0, header.attrLength);
            prv -> meta.num--;
            writeHeader(prv);
        }
    } else if (node -> meta.nxt && node -> meta.father == nxt -> meta.father && nxt -> meta.num + 1 >= header.minNum + 1) {
        // borrow from next sibling
        if (node -> meta.is_leaf) {
            /*
                1. update father
                2. update cur
                3. move sibling, clean head
            */
            for (int i = 0; i < father -> meta.num; i++) {
                if (EQ(nxt -> keys, father -> keys + i * header.attrLength,
                       nxt -> pages[0], father -> pages[i],
                       nxt -> slots[0], father -> slots[i],
                       header.attrType, header.attrLength)) {
                    father -> pages[i] = nxt -> pages[1];
                    father -> slots[i] = nxt -> slots[1];
                    memcpy(father -> keys + i * header.attrLength, nxt -> keys + header.attrLength, header.attrLength);

                    bpm -> markDirty(father -> index);
                    break;
                }
            }
            
            node -> pages[node -> meta.num] = nxt -> pages[0];
            node -> slots[node -> meta.num] = nxt -> slots[0];
            memcpy(node -> keys + node -> meta.num * header.attrLength, nxt -> keys, header.attrLength);
            node -> meta.num++;
            writeHeader(node);

            for (int i = 0; i < nxt -> meta.num - 1; i++) {
                // copy i + 1 to i
                nxt -> pages[i] = nxt -> pages[i + 1];
                nxt -> slots[i] = nxt -> slots[i + 1];
                memcpy(nxt -> keys + i * header.attrLength, nxt -> keys + (i + 1) * header.attrLength, header.attrLength);
            }
            nxt -> pages[nxt -> meta.num - 1] = 0;
            nxt -> slots[nxt -> meta.num - 1] = 0;
            memset(nxt -> keys + (nxt -> meta.num - 1) * header.attrLength, 0, header.attrLength);

            nxt -> meta.num--;
            writeHeader(nxt);
        } else {
            int r = rank(father, nxt -> keys, nxt -> pages[0], nxt -> slots[0]) - 1;
            
            node -> pages[node -> meta.num] = father -> pages[r];
            node -> slots[node -> meta.num] = father -> slots[r];
            memcpy(node -> keys + node -> meta.num * header.attrLength, father -> keys + r * header.attrLength, header.attrLength);
            node -> children[node -> meta.num + 1] = nxt -> children[0];
            node -> meta.num++;
            writeHeader(node);

            BLinkNode *c = makeNode(nxt -> children[0]);
            c -> meta.father = node -> meta.id;
            writeHeader(c);
            delete c;

            father -> pages[r] = nxt -> pages[0];
            father -> slots[r] = nxt -> slots[0];
            memcpy(father -> keys + r * header.attrLength, nxt -> keys, header.attrLength);
            bpm -> markDirty(father -> index);

            for (int i = 0; i < nxt -> meta.num - 1; i++) {
                // copy i + 1 to i
                nxt -> pages[i] = nxt -> pages[i + 1];
                nxt -> slots[i] = nxt -> slots[i + 1];
                memcpy(nxt -> keys + i * header.attrLength, nxt -> keys + (i + 1) * header.attrLength, header.attrLength);
                nxt -> children[i] = nxt -> children[i + 1];
            }
            nxt -> children[nxt -> meta.num - 1] = nxt -> children[nxt -> meta.num];
            nxt -> pages[nxt -> meta.num - 1] = 0;
            nxt -> slots[nxt -> meta.num - 1] = 0;
            nxt -> children[nxt -> meta.num] = 0;
            memset(nxt -> keys + (nxt -> meta.num - 1) * header.attrLength, 0, header.attrLength);
            nxt -> meta.num--;
            writeHeader(nxt);
        }
    } else {
        // merge
        if (node -> meta.prv && prv -> meta.father == node -> meta.father) {
            // merge with previous
            int r = rank(father, node -> keys, node -> pages[0], node -> slots[0]) - 1;

            if (node -> meta.is_leaf) {
                memcpy(prv -> pages + prv -> meta.num, node -> pages, (node -> meta.num) << 2);
                memcpy(prv -> slots + prv -> meta.num, node -> slots, (node -> meta.num) << 2);
                memcpy(prv -> keys + (prv -> meta.num) * header.attrLength, node -> keys, (node -> meta.num) * header.attrLength);
                prv -> meta.num += node -> meta.num;
            } else {
                for (int i = 0; i < node -> meta.num + 1; i++) {
                    BLinkNode *c = makeNode(node -> children[i]);
                    c -> meta.father = prv -> meta.id;
                    writeHeader(c);
                    delete c;
                }

                prv -> pages[prv -> meta.num] = father -> pages[r];
                prv -> slots[prv -> meta.num] = father -> slots[r];
                memcpy(prv -> keys + (prv -> meta.num) * header.attrLength, father -> keys + r * header.attrLength, header.attrLength);

                memcpy(prv -> pages + prv -> meta.num + 1, node -> pages, (node -> meta.num) << 2);
                memcpy(prv -> slots + prv -> meta.num + 1, node -> slots, (node -> meta.num) << 2);
                memcpy(prv -> keys + (prv -> meta.num + 1) * header.attrLength, node -> keys, (node -> meta.num) * header.attrLength);
                memcpy(prv -> children + (prv -> meta.num + 1), node -> children, (node -> meta.num + 1) << 2);
                prv -> meta.num += node -> meta.num + 1;
            }

            prv -> meta.nxt = node -> meta.nxt;
            if (node -> meta.nxt) {
                nxt -> meta.prv = node -> meta.prv;
                writeHeader(nxt);
            }
            writeHeader(prv);

            for (int i = r; i < father -> meta.num - 1; i++) {
                father -> pages[i] = father -> pages[i + 1];
                father -> slots[i] = father -> slots[i + 1];
                memcpy(father -> keys + i * header.attrLength, father -> keys + (i + 1) * header.attrLength, header.attrLength);
                father -> children[i + 1] = father -> children[i + 2];
            }
            father -> meta.num--;
            writeHeader(father);

            if (father -> meta.num == 0) {
                prv -> meta.father = 0;
                writeHeader(prv);

                header.root = prv -> meta.id;
                int header_index;
                BufType header_buf = bpm -> getPage(fid, 0, header_index);
                bpm -> access(header_index);
                memcpy(header_buf, &header, sizeof(IX_FileHeader));
                bpm -> markDirty(header_index);
            }

        } else {
            // merge with next
            int r = rank(father, nxt -> keys, nxt -> pages[0], nxt -> slots[0]) - 1;

            if (node -> meta.is_leaf) {
                memcpy(node -> pages + node -> meta.num, nxt -> pages, (nxt -> meta.num) << 2);
                memcpy(node -> slots + node -> meta.num, nxt -> slots, (nxt -> meta.num) << 2);
                memcpy(node -> keys + (node -> meta.num) * header.attrLength, nxt -> keys, (nxt -> meta.num) * header.attrLength);
                node -> meta.num += nxt -> meta.num;
            } else {
                for (int i = 0; i < nxt -> meta.num + 1; i++) {
                    BLinkNode *c = makeNode(nxt -> children[i]);
                    c -> meta.father = node -> meta.id;
                    writeHeader(c);
                    delete c;
                }

                node -> pages[node -> meta.num] = father -> pages[r];
                node -> slots[node -> meta.num] = father -> slots[r];
                memcpy(node -> keys + (node -> meta.num) * header.attrLength, father -> keys + r * header.attrLength, header.attrLength);

                memcpy(node -> pages + node -> meta.num + 1, nxt -> pages, (nxt -> meta.num) << 2);
                memcpy(node -> slots + node -> meta.num + 1, nxt -> slots, (nxt -> meta.num) << 2);
                memcpy(node -> keys + (node -> meta.num + 1) * header.attrLength, nxt -> keys, (nxt -> meta.num) * header.attrLength);
                memcpy(node -> children + (node -> meta.num + 1), nxt -> children, (nxt -> meta.num + 1) << 2);
                node -> meta.num += nxt -> meta.num + 1;
            }


            node -> meta.nxt = nxt -> meta.nxt;
            if (nxt -> meta.nxt) {
                BLinkNode *nnxt = makeNode(nxt -> meta.nxt);
                nnxt -> meta.prv = node -> meta.id;
                writeHeader(nnxt);
                delete nnxt;
            }
            writeHeader(node);

            for (int i = r; i < father -> meta.num - 1; i++) {
                father -> pages[i] = father -> pages[i + 1];
                father -> slots[i] = father -> slots[i + 1];
                memcpy(father -> keys + i * header.attrLength, father -> keys + (i + 1) * header.attrLength, header.attrLength);
                father -> children[i + 1] = father -> children[i + 2];
            }
            father -> meta.num--;
            writeHeader(father);

            if (father -> meta.num == 0) {
                node -> meta.father = 0;
                writeHeader(node);

                header.root = node -> meta.id;
                int header_index;
                BufType header_buf = bpm -> getPage(fid, 0, header_index);
                bpm -> access(header_index);
                memcpy(header_buf, &header, sizeof(IX_FileHeader));
                bpm -> markDirty(header_index);
            }
        }
    }
    if (node -> meta.prv)
        delete prv;
    if (node -> meta.nxt)
        delete nxt; 
    delete node;
    mergeUp(father);
}

bool IX_IndexHandle::InsertEntry(void *indexData, int pid, int sid) {
    /*
        1. go down, check insert position and insert
        2. check split
    */
    std::pair<BLinkNode*, int> find_pos = find(header.root, indexData, pid, sid);
    BLinkNode *node = find_pos.first;
    int r = find_pos.second;
    if (r == -1)
        return false;
    int insert_id = node -> meta.id;
    for (int i = node -> meta.num - 1; i >= r; i--) {
        // copy i to i + 1
        node -> pages[i + 1] = node -> pages[i];
        node -> slots[i + 1] = node -> slots[i];
        node -> children[i + 2] = node -> children[i + 1];
        memcpy(node -> keys + (i + 1) * header.attrLength, node -> keys + i * header.attrLength, header.attrLength);
    }
    
    // insert to r
    node -> meta.num++;
    node -> pages[r] = pid;
    node -> slots[r] = sid;
    node -> children[r] = 0;
    memcpy(node -> keys + r * header.attrLength, indexData, header.attrLength);
    writeHeader(node);
    splitUp(node);
    return true;
}

bool IX_IndexHandle::DeleteEntry(void *indexData, int pid, int sid) {
    /*
        1. go down, check position to delete
        2. check merge
    */
    std::pair <BLinkNode*, int> find_pos = find(header.root, indexData, pid, sid);
    BLinkNode *node = find_pos.first;
    int r = find_pos.second - 1;
    if (r == -1)
        return false;
    if (!EQ(indexData, node -> keys + (r * header.attrLength),
            pid, node -> pages[r],
            sid, node -> slots[r],
            header.attrType, header.attrLength)) {
        return false;
    }
    if (r == 0 && node -> meta.num >= 2) {
        updateLeftUp(node -> meta.father,
                     node -> keys + header.attrLength, indexData,
                     node -> pages[1], pid,
                     node -> slots[1], sid);
    }
    for (int i = r; i < node -> meta.num - 1; i++) {
        // copy i + 1 to i
        node -> pages[i] = node -> pages[i + 1];
        node -> slots[i] = node -> slots[i + 1];
        memcpy(node -> keys + i * header.attrLength, node -> keys + (i + 1) * header.attrLength, header.attrLength);
    }
    node -> pages[node -> meta.num - 1] = 0;
    node -> slots[node -> meta.num - 1] = 0;
    memset(node -> keys + (node -> meta.num - 1) * header.attrLength, 0, header.attrLength);
    node -> meta.num--;
    writeHeader(node);
    mergeUp(node);
    return true;
}

bool IX_IndexHandle::OpenScan(void *indexData, CompOp cmp) {
    if (cmp == LT_OP) {
        // <
        auto res = find(header.root, indexData, -INF, -INF);
        node_id = res.first -> meta.id;
        r_id = res.second - 1;
        delete res.first;
        return r_id >= 0;
    } else if (cmp == GT_OP) {
        // >
        auto res = find(header.root, indexData, INF, INF);
        node_id = res.first -> meta.id;
        r_id = res.second;
        int node_size = res.first -> meta.num;
        delete res.first;
        return r_id < node_size;
    } else if (cmp == LE_OP) {
        // <=
        auto res = find(header.root, indexData, INF, INF);
        node_id = res.first -> meta.id;
        r_id = res.second - 1;
        delete res.first;
        return r_id >= 0;
    } else if (cmp == GE_OP || cmp == EQ_OP) {
        // >=
        auto res = find(header.root, indexData, -INF, -INF);
        node_id = res.first -> meta.id;
        r_id = res.second;
        int node_size = res.first -> meta.num;
        delete res.first;
        return r_id < node_size;
    } else if (cmp == NO_OP) {
        // all
        BLinkNode *node = findMin(header.root);
        node_id = node -> meta.id;
        r_id = 0;
        return true;
    } else {
        node_id = r_id = 0;
        return false;
    }
}

bool IX_IndexHandle::GetNextRecord(int &pid, int &sid) {
    if (node_id == 0) {
        return false;
    }
    BLinkNode *node = makeNode(node_id);
    pid = node -> pages[r_id], sid = node -> slots[r_id];
    if (r_id == node -> meta.num - 1) {
        node_id = node -> meta.nxt;
        if (node_id) {
            BLinkNode *nxt = makeNode(node_id);
            r_id = 0;
            delete nxt;
        }
    } else {
        r_id++;
    }
    delete node;
    return true;
}

bool IX_IndexHandle::GetPrevRecord(int &pid, int &sid) {
    if (node_id == 0) {
        return false;
    }
    BLinkNode *node = makeNode(node_id);
    pid = node -> pages[r_id], sid = node -> slots[r_id];
    if (r_id == 0) {
        node_id = node -> meta.prv;
        if (node_id) {
            BLinkNode *prv = makeNode(node_id);
            r_id = prv -> meta.num - 1;
            delete prv;
        }
    } else {
        r_id--;
    }
    delete node;
    return true;
}

bool IX_IndexHandle::HasRecord(void *indexData) {
    std::pair<BLinkNode *, int> find_pos = find(header.root, indexData, INF, INF);
    BLinkNode *node = find_pos.first;
    int r = find_pos.second;
    if (r == -1) {
        delete node;
        return false;
    }
    void *data = (void*) (node -> keys + (r - 1) * header.attrLength);
    if (memcmp(data, indexData, header.attrLength) == 0) {
        delete node;
        std::cout << *(unsigned int*)data <<" " << *(unsigned int*)indexData <<" got true!" << std::endl;
        return true;
    } else {
        delete node;
        return false;
    }
}