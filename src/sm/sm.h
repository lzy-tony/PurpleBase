#ifndef SM_H
#define SM_H

#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#include <set>
#include <map>
#include <string>
#include <vector>

class AttrMeta {
public:
    string attrName;
    AttrType attrType;
    int attrLength;
    int original_attrLength;
    int offset;
    bool isNotNULL;
    bool isPrimary;
    bool isUnique;
    bool isIndex;
    bool isForeign;
    std::string referenceTable;
    std::string foreignKeyName;
};

class TableMeta {
public:
    std::string tableName;
    int attrNum;
    int recordSize;
    std::vector <AttrMeta> attrs;
    std::set <std::string> foreignKeyTableName;
};

class SM_Manager {
private:
    FileManager *fm;
    BufPageManager *bpm;
    RM_Manager *rm;
    IX_Manager *ix;

    inline int table_name_to_id(const std::string name);
    inline bool foreignKeyOnTable(const std::string name);

public:
    std::string DBName;
    int tableNum;
    std::vector <TableMeta> tables;
    std::map <std::string, int> table_to_fid;

    SM_Manager(FileManager *_fm, BufPageManager *_bpm, RM_Manager *_rm, IX_Manager *_ix);

    void OpenDB(const std::string name);
    void CloseDB();
    void ShowTables();
    void CreateTable(TableMeta *table);
    void DropTable(const std::string name);
    void DescribeTable(const std::string name);
    void CreateIndex(const std::string tableName, std::string attrName);
    void DropIndex(const std::string tableName, std::string attrName);
    void AddPrimaryKey(const std::string tableName, std::string attrName);
    void DropPrimaryKey(const std::string tableName);
    void AddForeignKey(const std::string tableName, std::string attrName,
                       const std::string refTableName, std::string refAttrName);
    void DropForeignKey(const std::string tableName, std::string attrName);
};

#endif