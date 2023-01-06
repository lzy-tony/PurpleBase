#include <iostream>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <ctime>
#include <set>
#include <random>
#include <cassert>

#include "ix.h"

int main() {
    srand(time(NULL));
    FileManager *fm = new FileManager();
    BufPageManager *bpm = new BufPageManager(fm);
    IX_Manager *ix = new IX_Manager(fm, bpm);

    bool createFlag = ix -> CreateIndex("test", "scan", 4, INT_ATTRTYPE);
    assert (createFlag == true);
    printf("INDEX CREATED\n");

    int fid;
    bool openFlag = ix -> OpenIndex("test", "scan", fid);
    assert (openFlag == true);
    printf("INDEX OPENED\n");

    IX_IndexHandle *handle = new IX_IndexHandle(fm, bpm, fid);

    int s = 10000;

    for (int i = 1; i <= s; i++) {
        int *data = new int(i);
        bool insertFlag = handle -> InsertEntry(data, i, i);
        assert (insertFlag == true);
        insertFlag = handle -> InsertEntry(data, i + s, i + s);
        assert (insertFlag == true);
    }

    // LT_OP, <
    int *x = new int(rand() % s + 1);
    handle -> OpenScan(x, LT_OP);
    int cnt = 0;
    int pid, sid;
    bool getFlag;
    for (int i = *x - 1; i >= 1; i--) {
        getFlag = handle -> GetPrevRecord(pid, sid);
        std::cout << "i = " << i << " pid = " << pid << " sid = " << sid << std::endl;
        assert (getFlag == true);
        assert (pid == i + s);
        assert (sid == i + s);

        getFlag = handle -> GetPrevRecord(pid, sid);
        std::cout << "i = " << i << " pid = " << pid << " sid = " << sid << std::endl;
        assert (getFlag == true);
        assert (pid == i);
        assert (sid == i);
    }
    getFlag = handle -> GetPrevRecord(pid, sid);
    assert (getFlag == false);

    bool closeFlag = ix -> CloseIndex(fid);
    assert (closeFlag == true);

    bool removeFlag = ix -> DestroyIndex("test", "scan");
    assert (removeFlag == true);

    printf("INDEX DESTROYED\n");


    return 0;
}