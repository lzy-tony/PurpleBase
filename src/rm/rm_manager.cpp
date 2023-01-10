#include <cstring>

#include "rm.h"


RM_Manager::RM_Manager(FileManager *_fm, BufPageManager *_bpm) {
    fm = _fm, bpm = _bpm;
}

bool RM_Manager::CreateFile(const char *fileName, int recordSize) {
    /*
        1. create file with filename
        2. create header containing meta data of file
        3. use bpm to write back to page
    */
    if (!fm -> createFile((std::string(fileName)).c_str()))
        return false;
    int fid, index;
    if (!fm -> openFile(fileName, fid))
        return false;
    RM_FileHeader header;
    while (recordSize & 3) // fill up to 4
        recordSize++;
    header.recordSize = recordSize >> 2; // 4 bytes in sequence;
    header.recordPerPage = (PAGE_SIZE - sizeof(RM_PageHeader)) * 8 / (recordSize * 4 * 8 + 1) - 1; // record + 1 bit bitmap
    header.bitmapSize = (header.recordPerPage - 1) / 32 + 1;
    header.bitmapStart = sizeof(RM_PageHeader) / sizeof(int);
    header.totalPage = 1;
    header.freePage = 0;
    BufType buf = bpm -> getPage(fid, 0, index); // write header page
    memcpy(buf, &header, sizeof(header));
    bpm -> markDirty(index);
    bpm -> writeBack(index);
    fm -> closeFile(fid);
    return true;
}

bool RM_Manager::DestroyFile(const char *fileName) {
    /*
        1. close buffer and write back
        2. use system operation to remove file totally
    */
    bpm -> close();
    std::remove((std::string(fileName)).c_str());
    return true;
}

bool RM_Manager::OpenFile(const char *fileName, int &fid) {
    /*
        1. open file with fm
    */
    return fm -> openFile(fileName, fid);
}

bool RM_Manager::CloseFile(int fid) {
    bpm -> close();
    return !fm -> closeFile(fid);
}