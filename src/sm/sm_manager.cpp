#include <set>
#include <fstream>
#include <unistd.h>

#include "sm.h"

SM_Manager::SM_Manager(FileManager *_fm, BufPageManager *_bpm, RM_Manager *_rm, IX_Manager *_ix) {
    fm = _fm, bpm = _bpm, rm = _rm, ix = _ix;
}

void SM_Manager::OpenDB(const string name) {
    DBName.assign(name);
    chdir(DBName.c_str());
    ifstream db_meta("info.db");
    if (db_meta.fail()) {
        tableNum = 0;
    } else {
        db_meta >> tableNum;
    }
    tables.clear();
    table_to_fid.clear();
    for (int i = 0; i < tableNum; i++) {
        TableMeta table;
        db_meta >> table.tableName;
        db_meta >> table.attrNum;
        db_meta >> table.recordSize;

        table.attrs.clear();
        table.foreignKeyTableName.clear();
        for (int j = 0; j < table.attrNum; j++) {
            AttrMeta attr;
            std::string type;

            db_meta >> attr.attrName;
            db_meta >> attr.offset;
            db_meta >> attr.defaultValue;
            db_meta >> attr.original_attrLength;
            db_meta >> type;

            attr.isForeign = attr.isIndex = attr.isNotNULL = attr.isPrimary = false;

            if (type == "INT") {
                attr.attrType = INT_ATTRTYPE;
                attr.attrLength = sizeof(int);
            } else if (type == "FLOAT") {
                attr.attrType = FLOAT_ATTRTYPE;
                attr.attrLength = sizeof(float);
            } else if (type == "STRING") {
                attr.attrType = STRING_ATTRTYPE;
                db_meta >> attr.attrLength;
            }

            while(1) {
                std::string info;
                db_meta >> info;
                if (info == "PRIMARY") {
                    attr.isPrimary = true;
                } else if (info == "FOREIGN") {
                    attr.isForeign = true;
                    db_meta >> attr.referenceTable;
                    db_meta >> attr.foreignKeyName;
                    if (table.foreignKeyTableName.find(attr.referenceTable) == table.foreignKeyTableName.end()) {
                        table.foreignKeyTableName.insert(attr.referenceTable);
                    }
                } else if (info == "INDEX") {
                    attr.isIndex = true;
                } else if (info == "NOTNULL") {
                    attr.isNotNULL = true;
                } else if (info == "END") {
                    break;
                }
            }
            table.attrs.push_back(attr);
        }
        tables.push_back(table);
    }
    db_meta.close();
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
        int fid;
        rm -> OpenFile(iter -> tableName.c_str(), fid);
        table_to_fid[iter -> tableName] = fid;
    }
}

void SM_Manager::CloseDB() {
    ofstream db_meta("info.db");
    if (db_meta.fail()) {
        std::cerr << "Error: Cannot open meta info!" << std::endl;
        return;
    }
    db_meta << tableNum << "\n";
    for (auto table_iter = tables.begin(); table_iter != tables.end(); table_iter++) {
        db_meta << table_iter -> tableName << "\n";
        db_meta << table_iter -> attrNum << "\n";
        db_meta << table_iter -> recordSize << "\n";

        for (auto attr_iter = table_iter -> attrs.begin(); attr_iter != table_iter -> attrs.end(); attr_iter++) {
            db_meta << attr_iter -> attrName << "\n";
            db_meta << attr_iter -> offset << "\n";
            db_meta << attr_iter -> defaultValue << "\n";
            db_meta << attr_iter -> original_attrLength << "\n";
            
            if (attr_iter -> attrType == INT_ATTRTYPE) {
                db_meta << "INT\n";
            } else if (attr_iter -> attrType == FLOAT_ATTRTYPE) {
                db_meta << "FLOAT\n";
            } else if (attr_iter -> attrType == STRING_ATTRTYPE) {
                db_meta << "STRING\n";
                db_meta << attr_iter -> attrLength << "\n";
            }
            if (attr_iter -> isPrimary) {
                db_meta << "PRIMARY\n";
            }
            if (attr_iter -> isForeign) {
                db_meta << "FOREIGN\n";
                db_meta << attr_iter -> referenceTable << "\n";
                db_meta << attr_iter -> foreignKeyName << "\n";
            }
            if (attr_iter -> isIndex) {
                db_meta << "INDEX\n";
            }
            if (attr_iter -> isNotNULL) {
                db_meta << "NOTNULL\n";
            }
            db_meta << "END\n";
        }
    }
    db_meta.close();
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
        if (rm -> CloseFile(table_to_fid[iter -> tableName]) == false) {
            std::cerr << "Error: Failed to close table " << iter -> tableName << std::endl;
        }
    }
    tables.clear();
    table_to_fid.clear();
    chdir("..");
}

void SM_Manager::ShowTables() {
    std::cout << "Tables_in_" << DBName << std::endl;
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
        std::cout << iter -> tableName << std::endl;
    }
    std::cout << tableNum << " row(s) in set" << std::endl;
}

void SM_Manager::CreateTable(TableMeta *table) {
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
        if (iter -> tableName == table -> tableName) {
            std::cerr << "Error: Table exist!" << std::endl;
            return;
        }
    }
    int recordSize = 8;
    bool havePrimary = false;
    table -> foreignKeyTableName.clear();
    for (auto iter = table -> attrs.begin(); iter != table -> attrs.end(); iter++) {
        iter -> offset = recordSize >> 2;
        if (iter -> attrType == INT_ATTRTYPE) {
            iter -> original_attrLength = iter -> attrLength = sizeof(int);
        } else if (iter -> attrType == FLOAT_ATTRTYPE) {
            iter -> original_attrLength = iter -> attrLength = sizeof(float);
        } else if (iter -> attrType == STRING_ATTRTYPE) {
            iter -> original_attrLength = iter -> attrLength;
            while(iter -> attrLength % 4) {
                iter -> attrLength++;
            }
        }
        
        if (iter -> isPrimary) {
            iter -> isIndex = true;
            iter -> isNotNULL = true;
        }
        if (iter -> isForeign) {
            iter -> isIndex = true;
            iter -> isNotNULL = true;
        }
        // validity check!
        if (iter -> isIndex && iter -> attrType != INT_ATTRTYPE) {
            std::cerr << "Error: Cannot add index for non-int type!" << std::endl;
            return;
        }
        if (iter -> isPrimary && havePrimary) {
            std::cerr << "Error: Cannot add multiple primary key for table!" << std::endl;
            return;
        }
        if (iter -> isPrimary) {
            havePrimary = true;
        }
        if (iter -> isForeign) {
            if (iter -> referenceTable == table -> tableName) {
                std::cerr << "Error: Cannot reference to same table!" << std::endl;
                return;
            }
            bool fk_valid = false;
            for (auto r_iter = tables.begin(); r_iter != tables.end(); r_iter++) {
                if (r_iter -> tableName == iter -> referenceTable) {
                    for (auto attr_iter = r_iter -> attrs.begin(); attr_iter != r_iter -> attrs.end(); attr_iter++) {
                        if (attr_iter -> isPrimary && attr_iter -> attrName == iter -> foreignKeyName) {
                            fk_valid = true;
                            iter -> attrType = attr_iter -> attrType;
                            iter -> attrLength = attr_iter -> attrLength;
                            table -> foreignKeyTableName.insert(iter -> referenceTable);
                            break;
                        }
                    }
                    if (fk_valid)
                        break;
                }
            }
            if (fk_valid == false) {
                std::cerr << "Error: Invalid foreign key!" << std::endl;
                return;
            }
        }
        recordSize += iter -> attrLength;
    }
    table -> recordSize = recordSize;
    tables.push_back(*table);
    tableNum++;
    int fid;
    rm -> CreateFile(table -> tableName.c_str(), recordSize);
    rm -> OpenFile(table -> tableName.c_str(), fid);
    table_to_fid[table -> tableName] = fid;
    
    for (auto iter = table -> attrs.begin(); iter != table -> attrs.end(); iter++) {
        if (iter -> isIndex) {
            ix -> CreateIndex(table -> tableName.c_str(), iter -> attrName.c_str(), iter -> attrLength, iter -> attrType);
        }
    }
}

void SM_Manager::DropTable(const std::string name) {
    int tid = table_name_to_id(name);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    if (foreignKeyOnTable(name)) {
        std::cerr << "Error: Foreign key on table!" << std::endl;
        return;
    }
    if (rm -> CloseFile(table_to_fid[name]) == false) {
        std::cerr << "Error: Failed to close table" << std::endl;
        return;
    }
    for (auto iter = tables[tid].attrs.begin(); iter != tables[tid].attrs.end(); iter++) {
        if (iter -> isIndex) {
            ix -> DestroyIndex(name.c_str(), iter -> attrName.c_str());
        }
    }
    rm -> DestroyFile(name.c_str());
    tables.erase(tables.begin() + tid);
    tableNum--;
}

void SM_Manager::DescribeTable(const std::string name) {
    int tid = table_name_to_id(name);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }

    std::cout << "| Field | Type | Null | Default |" << std::endl;

    for (auto iter = tables[tid].attrs.begin(); iter != tables[tid].attrs.end(); iter++) {
        std::cout << "| " << iter -> attrName << " | ";
        if (iter -> attrType == INT_ATTRTYPE) {
            std::cout << "INT | ";
        } else if (iter -> attrType == FLOAT_ATTRTYPE) {
            std::cout << "FLOAT | ";
        } else if (iter -> attrType == STRING_ATTRTYPE) {
            std::cout << "VARCHAR(" << iter -> original_attrLength << ") | ";
        }
        if (iter -> isNotNULL) {
            std::cout << "NO | " << iter -> defaultValue << " |" << std::endl;
        } else {
            std::cout << " YES | " << iter -> defaultValue << " |" << std::endl;
        }
    }

    for (auto iter = tables[tid].attrs.begin(); iter != tables[tid].attrs.end(); iter++) {
        if (iter -> isPrimary) {
            std::cout << "PRIMARY KEY " << iter -> attrName << ";" << std::endl;
        }
    }

    for (auto iter = tables[tid].attrs.begin(); iter != tables[tid].attrs.end(); iter++) {
        if (iter -> isForeign) {
            std::cout << "FOREIGN KEY " << iter -> attrName
                      <<" REFERENCES " << iter -> referenceTable
                      << "(" << iter -> foreignKeyName << ");" << std::endl;
        }
    }

    // TODO: index key print according to sort
    for (auto iter = tables[tid].attrs.begin(); iter != tables[tid].attrs.end(); iter++) {
        if (iter -> isIndex && !iter -> isPrimary) {
            std::cout << "INDEX (" << iter -> attrName << ");" << std::endl;
        }
    }

}

void SM_Manager::CreateIndex(const std::string tableName, std::string attrName) {
    int tid = table_name_to_id(tableName);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    int attr_id = -1;
    for (int i = 0; i < tables[tid].attrNum; i++) {
        if (tables[tid].attrs[i].attrName == attrName) {
            attr_id = i;
            break;
        }
    }
    if (attr_id == -1) {
        std::cerr << "Error: Column not found!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].attrType != INT_ATTRTYPE) {
        std::cerr << "Error: Index should be int!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isNotNULL == false) {
        std::cerr << "Error: Colomn should be not null!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isIndex == true) {
        std::cerr << "Error: Index already exist for column!" << std::endl;
        return;
    }
    tables[tid].attrs[attr_id].isIndex = true;

    // Create index for record
    int fid = table_to_fid[tableName];
    RM_FileHandle *rm_filehandle = new RM_FileHandle(fm, bpm, fid);
    RM_FileScan *rm_filescan = new RM_FileScan(fm, bpm);
    if (!rm_filescan -> OpenScan(rm_filehandle, 0, 0, NO_OP, NULL)) {
        return;
    }
    
    ix -> CreateIndex(tableName.c_str(), attrName.c_str(), tables[tid].attrs[attr_id].attrLength, tables[tid].attrs[attr_id].attrType);
    int iid;
    ix -> OpenIndex(tableName.c_str(), attrName.c_str(), iid);
    IX_IndexHandle *ix_handle = new IX_IndexHandle(fm, bpm, iid);

    int pid, sid;
    BufType data = new unsigned int[rm_filehandle -> header.recordSize];
    while (rm_filescan -> GetNextRecord(pid, sid, data)) {
        BufType ix_data = data + tables[tid].attrs[attr_id].offset;
        ix_handle -> InsertEntry((void *) ix_data, pid, sid);
    }
    delete [] data;
    delete rm_filehandle;
    delete rm_filescan;
    delete ix_handle;
    ix -> CloseIndex(iid);
}

void SM_Manager::DropIndex(const std::string tableName, std::string attrName) {
    int tid = table_name_to_id(tableName);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    int attr_id = -1;
    for (int i = 0; i < tables[tid].attrNum; i++) {
        if (tables[tid].attrs[i].attrName == attrName) {
            attr_id = i;
            if (tables[tid].attrs[i].isIndex == false) {
                std::cerr << "Error: No such index!" << std::endl;
                return;
            }
            if (tables[tid].attrs[i].isPrimary == true) {
                std::cerr << "Error: Primary constraint, cannot drop index!" << std::endl;
                return;
            }
            if (tables[tid].attrs[i].isForeign == true) {
                std::cerr << "Error: Foreign constraint, cannot drop index!" << std::endl;
                return;
            }
            break;
        }
    }
    if (attr_id == -1) {
        std::cerr << "Error: Column not found!" << std::endl;
        return;
    }
    ix -> DestroyIndex(tableName.c_str(), attrName.c_str());
    tables[tid].attrs[attr_id].isIndex = false;
}

void SM_Manager::AddPrimaryKey(const std::string tableName, std::string attrName) {
    int tid = table_name_to_id(tableName);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    int attr_id = -1;
    for (int i = 0; i < tables[tid].attrNum; i++) {
        if (tables[tid].attrs[i].isPrimary) {
            std::cerr << "Error: Primary key already exist for table!" << std::endl;
            return;
        }
        if (tables[tid].attrs[i].attrName == attrName) {
            attr_id = i;
        }
    }
    if (attr_id == -1) {
        std::cerr << "Error: Column not found!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].attrType != INT_ATTRTYPE) {
        std::cerr << "Error: Primary key should be int!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isNotNULL == false) {
        std::cerr << "Error: Colomn should be not null!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isForeign) {
        std::cerr << "Error: Primary key should not be foreign key!" << std::endl;
        return;
    }
    bool already_index = tables[tid].attrs[attr_id].isIndex;
    tables[tid].attrs[attr_id].isPrimary = true;
    tables[tid].attrs[attr_id].isIndex = true;

    int fid = table_to_fid[tableName];
    RM_FileHandle *rm_filehandle = new RM_FileHandle(fm, bpm, fid);
    RM_FileScan *rm_filescan = new RM_FileScan(fm, bpm);
    int pid, sid;
    BufType data = new unsigned int[rm_filehandle -> header.recordSize];
    std::set <int> pk;
    rm_filescan -> OpenScan(rm_filehandle, 0, 0, NO_OP, NULL);
    while (rm_filescan -> GetNextRecord(pid, sid, data)) {
        int record = *(data + tables[tid].attrs[attr_id].offset);
        if (pk.find(record) != pk.end()) {
            std::cerr << "Error: Duplicate value for targeted column!" << std::endl;
            delete [] data;
            delete rm_filehandle;
            delete rm_filescan;
            return;
        }
        pk.insert(record);
    }

    if (already_index) {
        delete [] data;
        delete rm_filehandle;
        delete rm_filescan;
        return;
    }

    // Create index for record
    rm_filescan -> OpenScan(rm_filehandle, 0, 0, NO_OP, NULL);
    ix -> CreateIndex(tableName.c_str(), attrName.c_str(), tables[tid].attrs[attr_id].attrLength, tables[tid].attrs[attr_id].attrType);
    int iid;
    ix -> OpenIndex(tableName.c_str(), attrName.c_str(), iid);
    IX_IndexHandle *ix_handle = new IX_IndexHandle(fm, bpm, iid);

    while (rm_filescan -> GetNextRecord(pid, sid, data)) {
        BufType ix_data = data + tables[tid].attrs[attr_id].offset;
        ix_handle -> InsertEntry((void *) ix_data, pid, sid);
    }
    delete [] data;
    delete rm_filehandle;
    delete rm_filescan;
    delete ix_handle;
    ix -> CloseIndex(iid);
}

void SM_Manager::DropPrimaryKey(const std::string tableName) {
    int tid = table_name_to_id(tableName);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    int attr_id = -1;
    for (int i = 0; i < tables[tid].attrNum; i++) {
        if (tables[tid].attrs[i].isPrimary) {
            attr_id = i;
            break;
        }
    }
    if (attr_id == -1) {
        std::cerr << "Error: No primary key!" << std::endl;
        return;
    }
    if (foreignKeyOnTable(tableName)) {
        std::cerr << "Error: Foreign key on table" << std::endl;
    }
    ix -> DestroyIndex(tableName.c_str(), tables[tid].attrs[attr_id].attrName.c_str());
    tables[tid].attrs[attr_id].isPrimary = false;
    tables[tid].attrs[attr_id].isIndex = false;
}

void SM_Manager::AddForeignKey(const std::string tableName, std::string attrName,
                               const std::string refTableName, std::string refAttrName) {
    int tid = table_name_to_id(tableName);
    int ref_tid = table_name_to_id(refTableName);
    if (tid == -1 || ref_tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    int attr_id = -1;
    int ref_attr_id = -1;
    for (int i = 0; i < tables[tid].attrNum; i++) {
        if (tables[tid].attrs[i].attrName == attrName) {
            attr_id = i;
            break;
        }
    }
    for (int i = 0; i < tables[ref_tid].attrNum; i++) {
        if (tables[ref_tid].attrs[i].attrName == refAttrName) {
            ref_attr_id = i;
            break;
        }
    }
    if (attr_id == -1 || ref_attr_id == -1) {
        std::cerr << "Error: Column not found!" << std::endl;
        return;
    }
    if (tables[ref_tid].attrs[ref_attr_id].isPrimary == false) {
        std::cerr << "Error: Referenced key not primary!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isForeign == true) {
        std::cerr << "Error: Foreign key already exist!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isNotNULL == false) {
        std::cerr << "Error: Foreign key should be not null!" << std::endl;
        return;
    }

    // Create index for record
    int fid = table_to_fid[tableName];
    RM_FileHandle *rm_filehandle = new RM_FileHandle(fm, bpm, fid);
    RM_FileScan *rm_filescan = new RM_FileScan(fm, bpm);
    int primary_iid;
    ix -> OpenIndex(refTableName.c_str(), refAttrName.c_str(), primary_iid);
    IX_IndexHandle *primary_ix_handle = new IX_IndexHandle(fm, bpm, primary_iid);
    int pid, sid;
    BufType data = new unsigned int[rm_filehandle -> header.recordSize];
    rm_filescan -> OpenScan(rm_filehandle, 0, 0, NO_OP, NULL);
    while (rm_filescan -> GetNextRecord(pid, sid, data)) {
        int record = *(data + tables[tid].attrs[attr_id].offset);
        if (!primary_ix_handle -> HasRecord(&record)) {
            std::cerr << "Error: Foreign key cannot be referenced to primary key!" << std::endl;
            ix -> CloseIndex(primary_iid);
            delete [] data;
            delete rm_filehandle;
            delete rm_filescan;
            delete primary_ix_handle;
            return;
        }
    }
    ix -> CloseIndex(primary_iid);
    delete primary_ix_handle;
    bool already_index = tables[tid].attrs[attr_id].isIndex;
    tables[tid].attrs[attr_id].isForeign = true;
    tables[tid].attrs[attr_id].isIndex = true;
    tables[tid].attrs[attr_id].referenceTable = refTableName;
    tables[tid].attrs[attr_id].foreignKeyName = refAttrName;
    tables[tid].foreignKeyTableName.insert(refTableName);
    if (already_index) {
        delete [] data;
        delete rm_filehandle;
        delete rm_filescan;
        return;
    }
    
    ix -> CreateIndex(tableName.c_str(), attrName.c_str(), tables[tid].attrs[attr_id].attrLength, tables[tid].attrs[attr_id].attrType);
    int iid;
    ix -> OpenIndex(tableName.c_str(), attrName.c_str(), iid);
    IX_IndexHandle *ix_handle = new IX_IndexHandle(fm, bpm, iid);

    rm_filescan -> OpenScan(rm_filehandle, 0, 0, NO_OP, NULL);
    while (rm_filescan -> GetNextRecord(pid, sid, data)) {
        BufType ix_data = data + tables[tid].attrs[attr_id].offset;
        ix_handle -> InsertEntry((void *) ix_data, pid, sid);
    }
    delete [] data;
    delete rm_filehandle;
    delete rm_filescan;
    delete ix_handle;
    ix -> CloseIndex(iid);
}

void SM_Manager::DropForeignKey(const std::string tableName, std::string attrName) {
    int tid = table_name_to_id(tableName);
    if (tid == -1) {
        std::cerr << "Error: No such table!" << std::endl;
        return;
    }
    int attr_id = -1;
    for (int i = 0; i < tables[tid].attrNum; i++) {
        if (tables[tid].attrs[i].attrName == attrName) {
            attr_id = i;
            break;
        }
    }
    if (attr_id == -1) {
        std::cerr << "Error: Column not found!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isForeign == false) {
        std::cerr << "Error: Not a foreign key!" << std::endl;
        return;
    }
    if (tables[tid].attrs[attr_id].isPrimary) {
        std::cerr << "Error: Is primary key!" << std::endl;
        return;
    }
    tables[tid].foreignKeyTableName.erase(tables[tid].attrs[attr_id].referenceTable);
    tables[tid].attrs[attr_id].referenceTable = "";
    tables[tid].attrs[attr_id].foreignKeyName = "";
    tables[tid].attrs[attr_id].isForeign = false;
    tables[tid].attrs[attr_id].isIndex = false;
    ix -> DestroyIndex(tableName.c_str(), attrName.c_str());
}

inline int SM_Manager::table_name_to_id(const std::string name) {
    for (int i = 0; i < tableNum; i++) {
        if (tables[i].tableName == name) {
            return i;
        }
    }
    return -1;
}

inline bool SM_Manager::foreignKeyOnTable(const std::string name) {
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
        if (iter -> foreignKeyTableName.find(name) != iter -> foreignKeyTableName.end()) {
            return true;
        }
    }
    return false;
}