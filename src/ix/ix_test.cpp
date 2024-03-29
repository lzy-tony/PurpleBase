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

    bool createFlag = ix -> CreateIndex("test", "ix", 4, FLOAT_ATTRTYPE);
    assert (createFlag == true);
    printf("INDEX CREATED\n");

    int fid;
    bool openFlag = ix -> OpenIndex("test", "ix", fid);
    assert (openFlag == true);
    printf("INDEX OPENED\n");

    IX_IndexHandle *handle = new IX_IndexHandle(fm, bpm, fid);

    default_random_engine e(time(NULL));
    uniform_real_distribution <float> u(-1000, 1000);

    std::set <std::pair<float, std::pair<int, int>>> records;
    std::pair<float, std::pair<int, int>> d[5000];
    std::pair<float, std::pair<int, int>> h[5000];
    for (int i = 0; i < 10000; i++) {
        float *data = new float(u(e) * u(e) - u(e) * u(e));
        // float *data = new float(i + 1);
        bool insertFlag = handle -> InsertEntry(data, i, i);
        assert (insertFlag == true);
        d[i % 5000] = std::make_pair(*data, std::make_pair(i, i));
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
        // std::cerr << "inserted i = " << i << " data = " << *data << std::endl;
        // handle -> print(handle -> header.root);
        // std::cerr << "finish print" << std::endl << std::endl;
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
        records.insert(std::make_pair(*data, std::make_pair(i, i)));
    }

    float pre = 10000.0;
    for (auto iter = records.begin(); iter != records.end(); iter++) {
        // printf("%.10lf\n", iter -> first);
        if (pre == iter -> first) {
            std::cout << "NOT UNIQUE" << std::endl;
            return 0;
        }
        pre = iter -> first;
    }

    fflush(stdout);

    printf("INSERT 10000 RECORDS\n");

    std::random_shuffle(d, d + 5000);

    // handle -> print(handle -> header.root);

    for (int i = 0; i < 5000; i++) {
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
        // std::cerr << "delete i = " << i << " data = " << d[i].first << std::endl;
        bool deleteFlag = handle -> DeleteEntry(&d[i].first, d[i].second.first, d[i].second.second);
        assert (deleteFlag == true);
        // handle -> print(handle -> header.root);
        // std::cerr << "finish print" << std::endl << std::endl;
        // std::cerr << "+++++++++++++++++++++++++++++++++++" << std::endl;
        assert (!handle -> HasRecord(&d[i].first));
        auto iter = records.lower_bound(std::make_pair(d[i].first, std::make_pair(d[i].second.first, d[i].second.second)));
        records.erase(iter);
    }

    printf("DELETE 5000 RECORDS\n");

    handle -> OpenScan(NULL, NO_OP);
    printf("OPEN SCAN\n");

    int pid, sid;
    for (auto iter = records.begin(); iter != records.end(); iter++) {
        bool getFlag = handle -> GetNextRecord(pid, sid);
        assert (getFlag == true);
        assert (pid == iter -> second.first);
        assert (sid == iter -> second.second);
        float t = iter -> first;
        assert (handle -> HasRecord(&t) == true);
    }
    bool getFlag = handle -> GetNextRecord(pid, sid);
    assert (getFlag == false);

    bool closeFlag = ix -> CloseIndex(fid);
    assert (closeFlag == true);

    bool removeFlag = ix -> DestroyIndex("test", "ix");
    assert (removeFlag == true);

    printf("INDEX DESTROYED\n");

    return 0;
}