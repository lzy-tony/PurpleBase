#ifndef RM_H
#define RM_H

#include "../pf/pf.h"

enum CompOp {
    EQ_OP,
    LT_OP,
    GT_OP,
    LE_OP,
    GE_OP,
    NE_OP,
    NO_OP,
};

class RM_FileHeader {
public:
    int recordSize;
    int recordPerPage;
    int bitmapSize; // bitmap offset for pages
    int bitmapStart;
    int totalPage; // total page num
    int freePage; // first available page    
};

class RM_PageHeader {
public:
    int recordNum;
    int nextFreePage;
};

class RM_Manager {
private:
    FileManager *fm;
    BufPageManager *bpm; 

public:
    RM_Manager(FileManager *_fm, BufPageManager *_bpm);
    ~RM_Manager();

    bool CreateFile(const char *fileName, int recordSize);
    bool DestroyFile(const char *fileName);
    bool OpenFile(const char *fileName, int &fid);
    bool CloseFile(int fid);
};

class RM_FileHandle {
    /*
        initialize with an open file
        rm file handle needs to handle record related ops
    */
private:
    FileManager *fm;
    BufPageManager *bpm;

public:
    int fid;
    RM_FileHeader header;

    RM_FileHandle(FileManager *_fm, BufPageManager *_bpm, int _fid);
    ~RM_FileHandle();

    bool GetRecord(int pid, int sid, BufType record) const;
    bool InsertRecord(int &pid, int &sid, const BufType record);
    bool UpdateRecord(int pid, int sid, const BufType record);
    bool DeleteRecord(int pid, int sid);

    bool queryBitmap(BufType bitmap, int sid) const;
    bool modifyBitmap(BufType bitmap, int sid, int flag);
    int findZeroBitmap(BufType bitmap) const;
    int findNext(BufType bitmap, int sid);
};

class RM_FileScan {
private:
    FileManager *fm;
    BufPageManager *bpm;
    RM_FileHandle *handle;

    int pid, sid, fid;
    CompOp compOp;
    BufType value;
    int attrLength, attrOffset;

public:

    RM_FileScan(FileManager *_fm, BufPageManager *_bpm);
    bool OpenScan(RM_FileHandle *_handle, int _attrLength, int _attrOffset, 
                  CompOp _compOp, BufType _value);
    bool GetNextRecord(int &_pid, int &_sid, BufType data);
};

#endif