#include <cstring>

#include "rm.h"
#include "../pf/pf.h"

RM_FileScan::RM_FileScan(FileManager *_fm, BufPageManager *_bpm) {
    fm = _fm, bpm = _bpm;
}

bool RM_FileScan::OpenScan(RM_FileHandle *_handle, int _attrLength, int _attrOffset, 
                           CompOp _compOp, BufType _value) {
    handle = _handle;
    attrLength = _attrLength, attrOffset = _attrOffset;
    compOp = _compOp, value = _value;
    pid = 1, sid = 0, fid = handle -> fid;
    if (attrLength + attrOffset < (handle -> header.recordSize << 2)) {
        return false;
    }
    return true;
}

bool RM_FileScan::GetNextRecord(int &_pid, int &_sid, BufType data) {
    /*
        returns true if find current, false if no
    */
    while (1) {
        // exit can't find
        if (pid >= handle -> header.totalPage) {
            return false;
        }

        int index;
        BufType buf = bpm -> getPage(fid, pid, index);
        BufType bitmap = buf + handle -> header.bitmapStart;
        bpm -> access(index);
        int next_one = handle -> findNext(bitmap, sid);
        int next_one_pid = pid;
        if (next_one == -1) {
            sid = 0, pid += 1;
        } else {
            sid = (next_one + 1) % handle -> header.recordPerPage;
            pid = (sid == 0) ? pid + 1 : pid;
            BufType slot = buf + handle -> header.bitmapStart +
                                 handle -> header.bitmapSize +
                                 next_one * handle -> header.recordSize;
            // TODO: add more ops, possibly support types
            // TODO: potentially non aligned offsets?
            switch (compOp) {
                case EQ_OP:
                    if (memcmp(slot + attrOffset, value, attrLength) == 0) {
                        _pid = next_one_pid, _sid = next_one;
                        memcpy(data, slot, handle -> header.recordSize << 2);
                        return true;
                    }
                    break;
                case LT_OP:
                    break;
                case GT_OP:
                    break;
                case LE_OP:
                    break;
                case GE_OP:
                    break;
                case NE_OP:
                    if (memcmp(slot + attrOffset, value, attrLength) != 0) {
                        _pid = next_one_pid, _sid = next_one;
                        memcpy(data, slot, handle -> header.recordSize << 2);
                        return true;
                    }
                    break;
                case NO_OP:
                    _pid = next_one_pid, _sid = next_one;
                    memcpy(data, slot, handle -> header.recordSize << 2);
                    return true;
            }
        }
    }
}