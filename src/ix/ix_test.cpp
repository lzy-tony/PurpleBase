#include <iostream>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <ctime>
#include <set>
#include <cassert>

#include "ix.h"

int main() {
    srand(time(NULL));
    FileManager *fm = new FileManager();
    BufPageManager *bpm = new BufPageManager(fm);
    IX_Manager *ix = new IX_Manager(fm, bpm);

    bool createFlag = ix -> CreateIndex("test", "ix", 4, FLOAT_TYPE);
    assert (createFlag == true);
    printf("INDEX CREATED\n");

    int fid;
    bool openFlag = ix -> OpenIndex("test", "ix", fid);
    assert (openFlag == true);
    printf("INDEX OPENED\n");

    IX_IndexHandle *handle = new IX_IndexHandle(fm, bpm, fid);

    std::set <std::pair<float, std::pair<int, int>>> records;
    std::pair<float, std::pair<int, int>> d[5000];
    std::pair<float, std::pair<int, int>> h[5000];
    for (int i = 0; i < 40; i++) {
        // float *data = new float(rand() / 100.0);
        float *data = new float(i + 1);
        bool insertFlag = handle -> InsertEntry(data, i, i);
        assert (insertFlag == true);
        d[i % 5000] = std::make_pair(*data, std::make_pair(i, i));
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
        // std::cerr << "inserted i = " << i << " data = " << *data << std::endl;
        // handle -> print(handle -> header.root);
        // std::cerr << "finish print" << std::endl << std::endl;
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
    }

    std::random_shuffle(d, d + 40);

    handle -> print(handle -> header.root);

    for (int i = 0; i < 40; i++) {
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
        // std::cerr << "delete i = " << i << " data = " << d[i].first << std::endl;
        bool deleteFlag = handle -> DeleteEntry(&d[i].first, d[i].second.first, d[i].second.second);
        assert (deleteFlag == true);
        handle -> print(handle -> header.root);
        // std::cerr << "finish print" << std::endl << std::endl;
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
    }


    bool closeFlag = ix -> CloseIndex(fid);
    assert (closeFlag == true);

    bool removeFlag = ix -> DestroyIndex("test", "ix");
    assert (removeFlag == true);

    printf("INDEX DESTROYED\n");

    return 0;
}