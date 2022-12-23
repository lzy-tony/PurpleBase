#include <cstring>

#include "ix.h"


IX_Manager::IX_Manager(FileManager *_fm, BufPageManager *_bpm) {
    fm = _fm, bpm = _bpm;
}

bool IX_Manager::CreateIndex(const char *fileName, const char *attrName,
                             int attrLength, AttrType attrType) {
    /*
        1. create file with fileName "." attrName
        2. create header containing meta data of file
        3. create empty page for root
        4. use bpm to write back to page
    */
    auto name = ("../../runs/" + std::string(fileName) + "." + std::string(attrName));
    if (!fm -> createFile(name.c_str()))
        return false;
    int fid, index;
    if (!fm -> openFile(name.c_str(), fid))
        return false;
    IX_FileHeader header;
    header.attrLength = attrLength;
    header.attrType = attrType;
    // TODO: to larger setting
    // header.minNum = ;
    // header.maxNum = ;
    header.minNum = 3;
    header.maxNum = 5;
    header.pages_offset = sizeof(IX_PageHeader) / sizeof(int);
    header.slots_offset = header.pages_offset + (header.maxNum + 1);
    header.children_offset = header.slots_offset + (header.maxNum + 1);
    header.keys_offset = header.children_offset + (header.maxNum + 1);
    header.root = 1;
    header.totalPage = 2;

    // allocate new root page
    BufType new_page = bpm -> getPage(fid, 1, index);
    memset(new_page, 0, PAGE_SIZE);
    new_page[0] = 1; // is_leaf
    new_page[1] = 1; // page id
    bpm -> access(index);
    bpm -> markDirty(index);
    bpm -> writeBack(index);

    BufType buf = bpm -> getPage(fid, 0, index);
    memcpy(buf, &header, sizeof(header));
    bpm -> markDirty(index);
    bpm -> writeBack(index);

    fm -> closeFile(fid);

    return true;
}

bool IX_Manager::DestroyIndex(const char *fileName, const char *attrName) {
    auto name = ("../../runs/" + std::string(fileName) + "." + std::string(attrName));
    bpm -> close();
    std::remove(name.c_str());
    return true;
}

bool IX_Manager::OpenIndex(const char *fileName, const char *attrName, int &fid) {
    auto name = ("../../runs/" + std::string(fileName) + "." + std::string(attrName));
    return fm -> openFile(name.c_str(), fid);
}

bool IX_Manager::CloseIndex(int fid) {
    bpm -> close();
    return !fm -> closeFile(fid);
}