#include <cstring>

#include "rm.h"


bool RM_FileHandle::queryBitmap(BufType bitmap, int sid) const {
	if (sid >= header.recordPerPage) {
		return false;
	}
	return (bitmap[sid >> 5] & (1u << (sid & 31)));
}

bool RM_FileHandle::modifyBitmap(BufType bitmap, int sid, int flag) {
	if (sid >= header.recordPerPage) {
		return false;
	}
	if (flag) {
		bitmap[sid >> 5] |= (1u << (sid & 31));
	} else {
		bitmap[sid >> 5] &= (~(1u << (sid & 31)));
	}
	return true;
}

int RM_FileHandle::findZeroBitmap(BufType bitmap) const {
	for (int pos = 0; pos < header.recordPerPage; pos++) {
		if (!queryBitmap(bitmap, pos)) {
			return pos;
		}
	}
	return -1;
}

int RM_FileHandle::findNext(BufType bitmap, int sid) {
	for (sid; sid < header.recordPerPage; sid++) {
		if (queryBitmap(bitmap, sid)) {
			return sid;
		}
	}
	return -1;
}

RM_FileHandle::RM_FileHandle(FileManager *_fm, BufPageManager *_bpm, int _fid) {
    fm = _fm, bpm = _bpm, fid = _fid;
    int index;
    BufType header_buf = bpm -> getPage(fid, 0, index);
    bpm -> access(index);
    memcpy(&header, header_buf, sizeof(RM_FileHeader));
}

bool RM_FileHandle::GetRecord(int pid, int sid, BufType record) const {
    /*
        @param: pid: page id
        @param: sid: slot id
        @param: record: retrieve to addr

		1. get page from bpm, access flag
		2. check bitmap for validation
		3. goto slot, return record
    */
    if (pid >= header.totalPage) {
		return false;
	}
    int index;
	BufType page_buf = bpm -> getPage(fid, pid, index);
	bpm -> access(index);
	page_buf += header.bitmapStart;
	if (!queryBitmap(page_buf, sid)) {
		return false;
	}
	page_buf += header.bitmapSize + sid * header.recordSize;
	memcpy(record, page_buf, header.recordSize << 2);
	return true;
}

bool RM_FileHandle::InsertRecord(int &pid, int &sid, const BufType record) {
	/*
		1. check whether we need to allocate free page
		2. get pid and sid, set access flag
		3. update bitmap, write page
		4. update page header
		5. update file header
		5. mark dirty
	*/
	if (header.freePage == 0) {
		/*
			1. update file header meta data, mark dirty
			2. write page header meta data, mark dirty
		*/
		header.freePage = header.totalPage++;
		int index;
		BufType header_buf = bpm -> getPage(fid, 0, index);
		bpm -> access(index);
		memcpy(header_buf, &header, sizeof(RM_FileHeader));
		bpm -> markDirty(index);

		BufType page_buf = bpm -> getPage(fid, header.freePage, index);
		memset(page_buf, 0, PAGE_SIZE);
		bpm -> access(index);
		bpm -> markDirty(index);
	}
	pid = header.freePage;
	int index;
	BufType page_buf = bpm -> getPage(fid, pid, index);
	bpm -> access(index);
	sid = findZeroBitmap(page_buf + header.bitmapStart);
	if (sid == -1) {
		return false;
	}
	modifyBitmap(page_buf + header.bitmapStart, sid, 1);
	memcpy(page_buf + header.bitmapStart + header.bitmapSize + sid * header.recordSize, record, header.recordSize << 2);
	page_buf[0]++;
	if (page_buf[0] == header.recordPerPage) {
		header.freePage = page_buf[1];
	}
	bpm -> markDirty(index);
	BufType header_buf = bpm -> getPage(fid, 0, index);
	memcpy(header_buf, &header, sizeof(RM_FileHeader));
	bpm -> access(index);
	bpm -> markDirty(index);
	return true;
}

bool RM_FileHandle::UpdateRecord(int pid, int sid, const BufType record) {
	/*
		1. get page from bpm, access flag
		2. write page
		3. mark dirty
	*/
	if (pid >= header.totalPage) {
		return false;
	}
	int index;
	BufType page_buf = bpm -> getPage(fid, pid, index);
	bpm -> access(index);
	memcpy(page_buf + header.bitmapStart + header.bitmapSize + sid * header.recordSize, record, header.recordSize << 2);
	bpm -> markDirty(index);
	return true;
}

bool RM_FileHandle::DeleteRecord(int pid, int sid) {
	/*
		1. get page from bpm, access flag
		2. set bitmap, set page
		3. update page header meta data, mark dirty
		4. update file header meta data, mark dirty
	*/
	if (pid >= header.totalPage) {
		return false;
	}
	int index;
	BufType page_buf = bpm -> getPage(fid, pid, index);
	bpm -> access(index);
	modifyBitmap(page_buf + header.bitmapStart, sid, 0);
	memset(page_buf + header.bitmapStart + header.bitmapSize + sid * header.recordSize, 0, (header.recordSize) << 2);
	if (page_buf[0] == header.recordPerPage) {
		page_buf[1] = header.freePage;
		header.freePage = pid;
		int header_index;
		BufType header_buf = bpm -> getPage(fid, 0, header_index);
		memcpy(header_buf, &header, sizeof(RM_FileHeader));
		bpm -> access(header_index);
		bpm -> markDirty(header_index);
	}
	page_buf[0]--;
	bpm -> markDirty(index);
	return true;
}