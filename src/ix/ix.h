#ifndef IX_H
#define IX_H

#include "../pf/pf.h"
#include "../utils/macros.h"

class IX_FileHeader {
public:
    int maxNum;
    int minNum;
    int attrLength;
    AttrType attrType;
    int pages_offset;
    int slots_offset;
    int children_offset;
    int keys_offset;
    int root;
    int totalPage;
};

class IX_PageHeader {
public:
    int is_leaf;
    int id;
    int prv;
    int nxt;
    int father;
    int num; // number of keys
};

class BLinkNode {
public:
    IX_PageHeader meta;

    BufType data;
    BufType pages;
    BufType slots;
    BufType children;
    unsigned char *keys;
    int index;
};

class IX_Manager {
private:
    FileManager *fm;
    BufPageManager *bpm;

public:
    IX_Manager(FileManager *_fm, BufPageManager *_bpm);

    bool CreateIndex(const char *fileName, const char *attrName, int attrLength, AttrType attrType);
    bool DestroyIndex(const char *fileName, const char *attrName);
    bool OpenIndex(const char *fileName, const char *attrName, int &fid);
    bool CloseIndex(int fid);
};

class IX_IndexHandle {
private:
    FileManager *fm;
    BufPageManager *bpm;

    BLinkNode *makeNode(int pid);
    bool LT(void *iData1, void *iData2,
            int p1, int p2, 
            int s1, int s2, 
            AttrType attrType, int attrLength);
    bool EQ(void *iData1, void *iData2,
            int p1, int p2, 
            int s1, int s2, 
            AttrType attrType, int attrLength);
    int rank(BLinkNode *node, void *indexData, int pid, int sid);
    std::pair<BLinkNode*, int> find(int id, void *indexData, int pid, int sid);
    bool writeHeader(BLinkNode *node);
    void splitUp(BLinkNode *node);
    void mergeUp(BLinkNode *node);
    void updateLeftUp(int id, void *newData, void *oldData,
                      int newp, int oldp,
                      int news, int olds);

public:
    int fid;
    IX_FileHeader header;

    IX_IndexHandle(FileManager *_fm, BufPageManager *_bpm, int fid);

    void print(int id);
    bool InsertEntry(void *indexData, int pid, int sid);
    bool DeleteEntry(void *indexData, int pid, int sid);
};

#endif