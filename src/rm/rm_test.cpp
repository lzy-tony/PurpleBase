#include <iostream>
#include <cstdio>
#include <cstring>
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
    std::cerr << "FILE SUCCESSFULLY CREATED!\n" << std::endl;

    int fid;
    bool openFlag = rm -> OpenFile("test", fid);
    assert (openFlag == true);
    printf("FILE SUCCESSFULLY OPENED\n");

    RM_FileHandle * handle = new RM_FileHandle(fm, bpm, fid);


    bool closeFlag = rm -> CloseFile(fid);
    assert (closeFlag == true);

    bool removeFlag = rm -> DestroyFile("test");
    assert (removeFlag == true);
    printf("FILE SUCCESSFULLY DESTROYED!\n");

    return 0;
}