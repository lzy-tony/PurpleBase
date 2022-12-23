#include <iostream>
#include <cstdio>
#include <cstring>
#include <set>
#include <algorithm>
#include <ctime>
#include <cassert>
#include "rm.h"

int main() {
    srand(time(NULL));
    FileManager *fm = new FileManager();
    BufPageManager *bpm = new BufPageManager(fm);
    RM_Manager *rm = new RM_Manager(fm, bpm);

    bool createFlag = rm -> CreateFile("test", 8);
    assert (createFlag == true);
    printf("FILE CREATED\n");

    int fid;
    bool openFlag = rm -> OpenFile("test", fid);
    assert (openFlag == true);
    printf("FILE OPENED\n");

    RM_FileHandle *handle = new RM_FileHandle(fm, bpm, fid);
    std::set <std::pair<double, std::pair<int, int>>> records;
    for (int i = 0; i < 1000; i++) {
        int pid, sid;
        double *data = new double(rand() / 100.0);
        bool insertFlag = handle -> InsertRecord(pid, sid, (BufType) data);
        assert (insertFlag == true);

        records.insert(std::make_pair(*data, std::make_pair(pid, sid)));
        // printf("%d %d %lf\n", pid, sid, *data);        

        double *getData = new double;
        bool getFlag = handle -> GetRecord(pid, sid, (BufType) getData);
        assert (getFlag == true);
        assert (*data == *getData);
    }
    printf("INSERT 1000 RECORDS\n");

    for (int i = 0; i < 500; i++) {
        double *data = new double(rand() / 100.0);
        auto iter = records.lower_bound(std::make_pair(*data, std::make_pair(-1, -1)));
        if (iter == records.end())
            continue;
        bool deleteFlag = handle -> DeleteRecord(iter -> second.first, iter -> second.second);
        assert (deleteFlag == true);
        records.erase(iter);
    }
    printf("DELETE 500 RECORDS\n");

    for (int i = 0; i < 1000; i++) {
        int pid, sid;
        double *data = new double(rand() / 100.0);
        bool insertFlag = handle -> InsertRecord(pid, sid, (BufType) data);
        assert (insertFlag == true);

        records.insert(std::make_pair(*data, std::make_pair(pid, sid)));
    }

    printf("RE INSERT 1000 RECORDS\n");

    for (int i = 0; i < 1000; i++) {
        double *old_data = new double(rand() / 100.0);
        double *new_data = new double(rand() / 100.0);
        auto iter = records.lower_bound(std::make_pair(*old_data, std::make_pair(-1, -1)));
        if (iter == records.end())
            continue;
        int pid = iter -> second.first, sid = iter -> second.second;
        bool updateFlag = handle -> UpdateRecord(pid, sid, (BufType) new_data);
        assert (updateFlag == true);
        records.erase(iter);
        records.insert(std::make_pair(*new_data, std::make_pair(pid, sid)));
    }

    printf("UPDATE 1000 RECORDS\n");

    RM_FileScan *scan = new RM_FileScan(fm, bpm);
    bool scanFlag = scan -> OpenScan(handle, 8, 0, NO_OP, NULL);
    assert (scanFlag == true);

    printf("OPEN SCAN\n");

    int pid, sid;
    double *data = new double;
    while (scan -> GetNextRecord(pid, sid, (BufType) data)) {
        auto iter = records.find(std::make_pair((double) *data, std::make_pair(pid, sid)));
        if (iter == records.end()) {
            printf("ERR: RECORD NOT FOUND\n");
            assert (0);
        }
        records.erase(iter);
    }
    assert (records.size() == 0);

    bool closeFlag = rm -> CloseFile(fid);
    assert (closeFlag == true);

    bool removeFlag = rm -> DestroyFile("test");
    assert (removeFlag == true);

    printf("FILE DESTROYED\n");

    return 0;
}