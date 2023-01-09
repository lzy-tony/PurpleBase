#include "ql.h"
#include <cstring>
#include <fstream>
#include <string>
#include <sstream>

inline int ql_manager::get_table_index(std::string& table_name){
    for (int i = 0; i < sm->tables.size(); i++){
        if(sm->tables[i].tableName == table_name){
            return i;
        }
    }
    return -1;
}

inline int ql_manager::get_column_index(int table_index, std::string& column_name){
    for(int i = 0; i < sm->tables[table_index].attrNum; i++){
        if(sm->tables[table_index].attrs[i].attrName == column_name) return i;
    }
    return -1;
}

inline void ql_manager::error_when_insert(input_attribute& insert_attribute, const char* error_reason){
    fprintf(stderr, "error occur when inserting values, reason: %s\n", error_reason);
    for (auto& value: insert_attribute.values){
        fprintf(stderr, "%s ",value.value.c_str());
    }
    fprintf(stderr, "\n");
}

inline bool ql_manager::match_type(int attr_type, int in_type){
    if(in_type == NULL_TYPE) return true;
    if(attr_type == INT_ATTRTYPE && in_type == INT_TYPE) return true;
    if(attr_type == FLOAT_ATTRTYPE && in_type == FLOAT_TYPE) return true;
    if(attr_type == STRING_ATTRTYPE && in_type == STRING_TYPE) return true;
    // if(attr_type == FLOAT_TYPE && in_type == INT_TYPE) return true;
    return false;
}

inline bool ql_manager::table_has_column(std::string& table_name, std::string& column_name){
    int table_index = get_table_index(table_name);
    if (table_index == -1) return false;
    int column_index = get_column_index(table_index, column_name);
    if (column_index == -1) return false;
    return true;
}

inline bool ql_manager::is_index_column(std::string& table_name,std::string& column_name){
    if(!table_has_column(table_name, column_name)) return false;
    int table_index = get_table_index(table_name);
    int column_index = get_column_index(table_index, column_name);
    return sm->tables[table_index].attrs[column_index].isIndex;
}

inline bool ql_manager::need_next(int op){
    if(op==EQUAL||op==GREATER||op==GREATER_EQUAL) return true;
    return false;
}

inline bool ql_manager::comp_op(float a, float b, int op){
    switch (op)
    {
    case EQUAL:
        return a==b;
        break;
    case LESS:
        return a < b;
        break;
    case GREATER:
        return a > b;
        break;
    case LESS_EQUAL:
        return a <= b;
        break;
    case GREATER_EQUAL:
        return a >= b;
        break;
    case NOT_EQUAL:
        return a != b;
        break;
    default:
        return true;
        break;
    }
}
inline bool ql_manager::comp_op(unsigned int a, unsigned int b, int op){
    switch (op)
    {
    case EQUAL:
        return a==b;
        break;
    case LESS:
        return a < b;
        break;
    case GREATER:
        return a > b;
        break;
    case LESS_EQUAL:
        return a <= b;
        break;
    case GREATER_EQUAL:
        return a >= b;
        break;
    case NOT_EQUAL:
        return a != b;
        break;
    default:
        return true;
        break;
    }
}
inline bool ql_manager::comp_op(std::string& a,std::string& b, int op){
    switch (op)
    {
    case EQUAL:
        return a==b;
        break;
    case LESS:
        return a < b;
        break;
    case GREATER:
        return a > b;
        break;
    case LESS_EQUAL:
        return a <= b;
        break;
    case GREATER_EQUAL:
        return a >= b;
        break;
    case NOT_EQUAL:
        return a != b;
        break;
    default:
        return true;
        break;
    }
}


inline int ql_manager::get_primary_key(int tid) {
    for (int i = 0; i < sm -> tables[tid].attrs.size(); i++) {
        if (sm -> tables[tid].attrs[i].isPrimary) {
            return i;
        }
    }
    return -1;
}

void ql_manager::Insert(InsertOp* insert_op){
    // check if table exist
    int table_index = get_table_index(insert_op -> table_name);
    if (table_index == -1){
        fprintf(stderr,"Error: no such table %s!\n", insert_op -> table_name.c_str());
        return;
    }
    TableMeta& this_table = sm->tables[table_index];
    InsertInfo* insert_info = dynamic_cast<InsertInfo*>(insert_op -> info);
    if (insert_info == nullptr){
        fprintf(stderr,"error when parsing insert info\n");
        return;
    }
    // prepare handlers
    int fid = sm -> table_to_fid[insert_op -> table_name];
    RM_FileHandle * rm_handle = new RM_FileHandle(fm, bpm, fid);
    IX_IndexHandle * index_handlers[this_table.attrNum];
    int idx_fid[this_table.attrNum];
    // FIXME if a column is both a pk and a fk, this may cause a error?
    for (int i = 0;i < this_table.attrNum; i++){
        if(this_table.attrs[i].isIndex){
            bool openFlag = ix -> OpenIndex(insert_op->table_name.c_str(), this_table.attrs[i].attrName.c_str(), idx_fid[i]);
            index_handlers[i] = new IX_IndexHandle(fm, bpm, idx_fid[i]);
        } else {
            index_handlers[i] = nullptr;
        }
    }

    IX_IndexHandle * foreign_index_handlers[this_table.attrNum];
    int foreign_idx_fid[this_table.attrNum];
    for (int i = 0;i < this_table.attrNum; i++){
        if(this_table.attrs[i].isForeign){
            int foreign_table_index = get_table_index(this_table.attrs[i].referenceTable);
            int foreign_primary_attr_index = -1;
            for (int i = 0; i<sm->tables[foreign_table_index].attrNum; i++){
                if(sm->tables[foreign_table_index].attrs[i].isPrimary) foreign_primary_attr_index = i;
            }
            bool openFlag = ix -> OpenIndex(this_table.attrs[i].referenceTable.c_str(), 
                sm->tables[foreign_table_index].attrs[foreign_primary_attr_index].attrName.c_str(), foreign_idx_fid[i]);
            foreign_index_handlers[i] = new IX_IndexHandle(fm, bpm, foreign_idx_fid[i]);
        } else {
            foreign_index_handlers[i] = nullptr;
        }
    }

    for (auto& insert_attribute : insert_info->attributes){
        // do the loop, insert one at a time
        if (insert_attribute.values.size() != this_table.attrNum){
            error_when_insert(insert_attribute, "attribute num not match");
            break;
        }
        // check attribute match
        bool type_check = true;
        for (int i = 0; i < insert_attribute.values.size(); i++){
            if(!match_type(this_table.attrs[i].attrType, insert_attribute.values[i].ret_type)){
                error_when_insert(insert_attribute, "attribute type not match");
                type_check = false;
                break;
            }
        }
        if (!type_check) break;
        // prepare insertion data
        bool insert_success = true;
        BufType data = new unsigned int[this_table.recordSize >> 2];
        for(int i = 0; i<(this_table.recordSize >> 2);i++){
            data[i] = 0;
        }

        unsigned long long *bitmap = (unsigned long long*)data;
        for (int i = 0; i < insert_attribute.values.size(); i++){
            // if primary check unique
            // if foreign key, check other exist
            // if not Null check null?
            if (this_table.attrs[i].isPrimary){
                int pk_index = atoi(insert_attribute.values[i].value.c_str());
                if(index_handlers[i] -> HasRecord(&pk_index)){
                    error_when_insert(insert_attribute, "unique constraint of primary key violated");
                    insert_success = false;
                    break;
                }
            }
            if (this_table.attrs[i].isForeign) {
                int fk_index = atoi(insert_attribute.values[i].value.c_str());
                if(!foreign_index_handlers[i] -> HasRecord(&fk_index)){
                    error_when_insert(insert_attribute, "referring to a foreign key that doesn't exist");
                    insert_success = false;
                    break;
                }
            }
            if (this_table.attrs[i].isNotNULL) {
                if(insert_attribute.values[i].ret_type == NULL_TYPE){
                    error_when_insert(insert_attribute, "insert Null into not Null coloumn");
                    insert_success = false;
                    break;
                }
            }
            if (this_table.attrs[i].attrType == STRING_ATTRTYPE) {
                if (insert_attribute.values[i].value.length() > this_table.attrs[i].original_attrLength && insert_attribute.values[i].ret_type == STRING_TYPE){
                    error_when_insert(insert_attribute, "exceed varchar length constraint");
                    insert_success = false;
                    break;
                }
            }
            if(insert_attribute.values[i].ret_type == INT_TYPE){
                *bitmap |= (1ull<<i);
                int inserted_value = atoi(insert_attribute.values[i].value.c_str());
                memcpy(data + this_table.attrs[i].offset, &inserted_value, 4);
            } else if (insert_attribute.values[i].ret_type == FLOAT_TYPE) {
                *bitmap |= (1ull<<i);
                float inserted_value = atof(insert_attribute.values[i].value.c_str());
                memcpy(data + this_table.attrs[i].offset, &inserted_value,4);
            } else if (insert_attribute.values[i].ret_type == STRING_TYPE) {
                *bitmap |= (1ull<<i);
                // copy the value into buffer, if there is spare space, add \0
                memset(data + this_table.attrs[i].offset, 0, sizeof(char) * this_table.attrs[i].attrLength);
                memcpy(data + this_table.attrs[i].offset, insert_attribute.values[i].value.c_str(), sizeof(char) * insert_attribute.values[i].value.length());
                // when taking out these attributes please add \0 in the end
            } else if (insert_attribute.values[i].ret_type == NULL_TYPE) {
                // DO nothing
                // no need to care about bitmap cause it has already be set to zero
            }
        }
        if(!insert_success){
            delete[] data;
            break;
        }

        // insert the record
        int pid, sid;
        bool insert_record_success = rm_handle -> InsertRecord(pid, sid, data);
        if(!insert_record_success) fprintf(stderr, "internal error occured when inserting record\n");
        
        // insert primary key index and all other index in the table
        for(int i = 0; i < insert_attribute.values.size();i++) {
            if(this_table.attrs[i].isIndex){
                index_handlers[i] -> InsertEntry(data+this_table.attrs[i].offset, pid, sid);
            }
        }
        delete[] data;
    }
    for (int i = 0;i < this_table.attrNum; i++){
        if(index_handlers[i]) {
            ix -> CloseIndex(idx_fid[i]);
            delete index_handlers[i];
        }
    }
    for (int i = 0;i < this_table.attrNum; i++){
        if(foreign_index_handlers[i]) {
            ix -> CloseIndex(foreign_idx_fid[i]);
            delete foreign_index_handlers[i];
        }
    }
    delete rm_handle;
}

void ql_manager::Delete(DeleteOp* delete_op){
    int table_index = get_table_index(delete_op -> table_name);
    if (table_index == -1){
        std::cerr << "Error: no such table " << delete_op -> table_name  << "!" << std::endl;
        return;
    }
    TableMeta& this_table = sm -> tables[table_index];

    WhereClauses* where_clauses;
    where_clauses = dynamic_cast<WhereClauses*>(delete_op -> info);
    // validate where clause columns
    std::vector <std::string> where_clause_table_names;
    where_clause_table_names.push_back(this_table.tableName);
    bool valid_clauses = validate_where_clause(where_clauses, where_clause_table_names);
    if (!valid_clauses) {
        std::cerr << "Error: error when parsing the where clauses, some of them are wrong" << std::endl;
        return;
    }
    
    // scan table, filter 
    /*
        1. open all index, open rm
        2. open all foreign key table index
        3. choose rule to select
        4. scan, (a) sort out where clause, (b) sort out foreign key constraint
    */
    std::vector <BufType> delete_data;
    std::vector <std::pair<int, int>> delete_pos;

    int fid = sm -> table_to_fid[delete_op -> table_name];
    RM_FileHandle *rm_handle = new RM_FileHandle(fm, bpm, fid);
    IX_IndexHandle *index_handlers[this_table.attrNum];
    int ix_fid[this_table.attrNum];
    for (int i = 0; i < this_table.attrNum; i++) {
        if (this_table.attrs[i].isIndex) {
            ix -> OpenIndex(delete_op -> table_name.c_str(), this_table.attrs[i].attrName.c_str(), ix_fid[i]);
            index_handlers[i] = new IX_IndexHandle(fm, bpm, ix_fid[i]);
        } else {
            index_handlers[i] = nullptr;
        }
    }

    std::vector <IX_IndexHandle*> foreign_handlers;
    std::vector <int> foreign_table_fids;
    for (int i = 0; i < sm -> tables.size(); i++) {
        if (i == table_index) {
            continue;
        }
        if (sm -> tables[i].foreignKeyTableName.find(this_table.tableName) == sm -> tables[i].foreignKeyTableName.end()) {
            continue;
        }
        int foreign_fid;
        std::string foreign_key_name;
        for (auto iter = sm -> tables[i].attrs.begin(); iter != sm -> tables[i].attrs.end(); iter++) {
            if (iter -> isForeign && iter -> referenceTable == delete_op -> table_name) {
                foreign_key_name = iter -> attrName;
                break;
            }
        }
        ix -> OpenIndex(sm -> tables[i].tableName.c_str(), foreign_key_name.c_str(), foreign_fid);
        IX_IndexHandle *handler = new IX_IndexHandle(fm, bpm, foreign_fid);
        foreign_handlers.push_back(handler);
        foreign_table_fids.push_back(foreign_fid);
    }
    
    int best_clause_index = -1;
    int clause_index = -1;
    bool error = false;
    for (int i = 0; i < where_clauses -> clauses.size(); i++){
        WhereClause& this_clause = where_clauses -> clauses[i];
        if (this_clause.is_operand && this_clause.operand == EQUAL 
            && is_index_column(this_clause.l_col.tablename, this_clause.l_col.column_name) && !this_clause.use_r_col){
                best_clause_index = i;
                break;
        }
        if (this_clause.is_operand && this_clause.operand != NOT_EQUAL 
            && is_index_column(this_clause.l_col.tablename, this_clause.l_col.column_name) && !this_clause.use_r_col){
                clause_index = i;
        }
    }
    if (best_clause_index != -1 || clause_index != -1) {
        // scan from ix
        int search_index = (best_clause_index == -1) ? clause_index : best_clause_index;
        WhereClause& this_clause = where_clauses -> clauses[search_index];
        int use_index = get_column_index(table_index, this_clause.l_col.column_name);
        BufType search_key = new unsigned int [1];
        *(int *) search_key = atoi(this_clause.r_val.value.c_str());
        index_handlers[use_index] -> OpenScan(search_key, (CompOp) this_clause.operand);

        while (1) {
            bool still_have = true;
            int pid, sid;
            if (need_next(this_clause.operand)) {
                still_have = index_handlers[use_index] -> GetNextRecord(pid, sid);
            } else {
                still_have = index_handlers[use_index] -> GetPrevRecord(pid, sid);
            }
            if (!still_have) break;
            BufType data = new unsigned int [sm -> tables[table_index].recordSize >> 2];
            rm_handle -> GetRecord(pid, sid, data);
            int match_res = match_record(data, table_index, where_clauses -> clauses, search_index);
            if (match_res == 2) {
                delete [] data;
                break;
            } else if (match_res == 1) {
                delete [] data;
            } else {
                // check foreign key constraint
                int primary_id = get_primary_key(table_index);
                if (primary_id != -1) {
                    BufType primary_data = &data[sm -> tables[table_index].attrs[primary_id].offset];
                    for (int i = 0; i < foreign_handlers.size(); i++) {
                        if (foreign_handlers[i] -> HasRecord(primary_data)) {
                            error = true;
                            std::cerr << "Error: foreign key constraint not matched, cannot delete!" << std::endl;
                            delete [] data;
                            break;
                        }
                    }
                }
                if (error) {
                    break;
                }
                delete_data.push_back(data);
                delete_pos.push_back(std::make_pair(pid, sid));
            }
        }
        delete [] search_key;
    } else {
        // scan from rm
        RM_FileScan *rm_scan = new RM_FileScan(fm, bpm);
        bool at_least_one = rm_scan -> OpenScan(rm_handle, 0, 0, NO_OP, NULL);
        int pid, sid;
        if (at_least_one) {
            while (1) {
                BufType data = new unsigned int [sm -> tables[table_index].recordSize >> 2];
                if (!rm_scan -> GetNextRecord(pid, sid, data)) {
                    delete [] data;
                    break;
                }
                int match_res = match_record(data, table_index, where_clauses -> clauses, -1);
                if (match_res == 2) {
                    delete [] data;
                    break;
                } else if (match_res == 1) {
                    delete [] data;
                } else {
                    // check foreign key constraint
                    int primary_id = get_primary_key(table_index);
                    if (primary_id != -1) {
                        BufType primary_data = &data[sm -> tables[table_index].attrs[primary_id].offset];
                        for (int i = 0; i < foreign_handlers.size(); i++) {
                            if (foreign_handlers[i] -> HasRecord(primary_data)) {
                                error = true;
                                std::cerr << "Error: foreign key constraint not matched, cannot delete!" << std::endl;
                                delete [] data;
                                break;
                            }
                        }
                    }
                    if (error) {
                        break;
                    }
                    delete_data.push_back(data);
                    delete_pos.push_back(std::make_pair(pid, sid));
                }
            }
        }
        delete rm_scan;
    }

    // delete all from delete data
    if (!error) {
        for (int i = 0; i < delete_data.size(); i++) {
            for (int j = 0; j < this_table.attrs.size(); j++) {
                if (this_table.attrs[j].isIndex) {
                    index_handlers[j] -> DeleteEntry(delete_data[i] + this_table.attrs[j].offset, delete_pos[i].first, delete_pos[i].second);
                }
            }
            rm_handle -> DeleteRecord(delete_pos[i].first, delete_pos[i].second);
        }
    }

    // clean up, close all indexes, delete data from vector
    delete rm_handle;
    for (int i = 0; i < this_table.attrNum; i++) {
        if (this_table.attrs[i].isIndex) {
            ix -> CloseIndex(ix_fid[i]);
            delete index_handlers[i];
        }
    }
    for (int i = 0; i < foreign_handlers.size(); i++) {
        ix -> CloseIndex(foreign_table_fids[i]);
        delete foreign_handlers[i];
    }
    // clean up data from vector
    for (int i = 0; i < delete_data.size(); i++) {
        delete [] delete_data[i];
    }
}

void ql_manager::Update(UpdateOp* update_op){
    int table_index = get_table_index(update_op -> table_name);
    if (table_index == -1){
        std::cerr << "Error: no such table " << update_op -> table_name  << "!" << std::endl;
        return;
    }
    TableMeta& this_table = sm -> tables[table_index];

    WhereClauses* where_clauses;
    where_clauses = dynamic_cast<WhereClauses*>(update_op -> info);
    bool update_primary = false;
    int primary_index;
    int new_primary;
    // validate where clause columns
    std::vector <std::string> where_clause_table_names;
    where_clause_table_names.push_back(this_table.tableName);
    bool valid_clauses = validate_where_clause(where_clauses, where_clause_table_names);
    if (!valid_clauses) {
        std::cerr << "Error: error when parsing the where clauses, some of them are wrong" << std::endl;
        return;
    }
    // validate set clause columns
    for (auto &set_clause: update_op -> set_clauses) {
        int col_index = get_column_index(table_index, set_clause.column_name);
        if (col_index == -1) {
            std::cerr << "Error: invalid column in set clause!" << std::endl;
            return;
        }
        if (this_table.attrs[col_index].isPrimary) {
            update_primary = true;
            primary_index = col_index;
            new_primary = atoi(set_clause.update_value.value.c_str());
        }
        if (set_clause.update_value.ret_type == INT_TYPE || 
            set_clause.update_value.ret_type == FLOAT_TYPE ||
            set_clause.update_value.ret_type == STRING_TYPE) {
            if (!match_type(this_table.attrs[col_index].attrType, set_clause.update_value.ret_type)) {
                std::cerr << "Error: type unmatch!" << std::endl;
                return;
            }
            if (set_clause.update_value.ret_type == STRING_TYPE &&
                set_clause.update_value.value.length() > this_table.attrs[col_index].original_attrLength) {
                std::cerr << "Error: varchar length exceed schema length!" << std::endl;
                return;
            }
        } else if (set_clause.update_value.ret_type == NULL_TYPE) {
            if (this_table.attrs[col_index].isNotNULL) {
                std::cerr << "Error: set null to not-null field!" << std::endl;
                return;
            }
        } else {
            std::cerr << "Error: invalid type!" << std::endl;
            return;
        }
        if (this_table.attrs[col_index].isForeign) {
            int new_foreign = atoi(set_clause.update_value.value.c_str());
            int ref_table_idx = get_table_index(this_table.attrs[col_index].referenceTable);
            int foreign_ix;
            ix -> OpenIndex(this_table.attrs[col_index].referenceTable.c_str(), this_table.attrs[col_index].foreignKeyName.c_str(), foreign_ix);
            IX_IndexHandle *foreign_handle = new IX_IndexHandle(fm, bpm, foreign_ix);
            if (!foreign_handle -> HasRecord(&new_foreign)) {
                std::cerr << "Error: update value cannot be referenced to foreign key!" << std::endl;
                delete foreign_handle;
                ix -> CloseIndex(foreign_ix);
                return;
            }
            delete foreign_handle;
            ix -> CloseIndex(foreign_ix);
        }
    }
    
    // scan table, filter 
    /*
        1. open all index, open rm
        2. open all foreign key table index
        3. choose rule to select
        4. scan, (a) sort out where clause, (b) sort out foreign key constraint
    */
    std::vector <BufType> delete_data;
    std::vector <std::pair<int, int>> delete_pos;

    int fid = sm -> table_to_fid[update_op -> table_name];
    RM_FileHandle *rm_handle = new RM_FileHandle(fm, bpm, fid);
    IX_IndexHandle *index_handlers[this_table.attrNum];
    int ix_fid[this_table.attrNum];
    for (int i = 0; i < this_table.attrNum; i++) {
        if (this_table.attrs[i].isIndex) {
            ix -> OpenIndex(update_op -> table_name.c_str(), this_table.attrs[i].attrName.c_str(), ix_fid[i]);
            index_handlers[i] = new IX_IndexHandle(fm, bpm, ix_fid[i]);
        } else {
            index_handlers[i] = nullptr;
        }
    }

    std::vector <IX_IndexHandle*> foreign_handlers;
    std::vector <int> foreign_table_fids;
    for (int i = 0; i < sm -> tables.size(); i++) {
        if (i == table_index) {
            continue;
        }
        if (sm -> tables[i].foreignKeyTableName.find(this_table.tableName) == sm -> tables[i].foreignKeyTableName.end()) {
            continue;
        }
        int foreign_fid;
        std::string foreign_key_name;
        for (auto iter = sm -> tables[i].attrs.begin(); iter != sm -> tables[i].attrs.end(); iter++) {
            if (iter -> isForeign && iter -> referenceTable == update_op -> table_name) {
                foreign_key_name = iter -> attrName;
                break;
            }
        }
        ix -> OpenIndex(sm -> tables[i].tableName.c_str(), foreign_key_name.c_str(), foreign_fid);
        IX_IndexHandle *handler = new IX_IndexHandle(fm, bpm, foreign_fid);
        foreign_handlers.push_back(handler);
        foreign_table_fids.push_back(foreign_fid);
    }
    
    int best_clause_index = -1;
    int clause_index = -1;
    bool error = false;
    for (int i = 0; i < where_clauses -> clauses.size(); i++){
        WhereClause& this_clause = where_clauses -> clauses[i];
        if (this_clause.is_operand && this_clause.operand == EQUAL 
            && is_index_column(this_clause.l_col.tablename, this_clause.l_col.column_name) && !this_clause.use_r_col){
                best_clause_index = i;
                break;
        }
        if (this_clause.is_operand && this_clause.operand != NOT_EQUAL 
            && is_index_column(this_clause.l_col.tablename, this_clause.l_col.column_name) && !this_clause.use_r_col){
                clause_index = i;
        }
    }
    if (best_clause_index != -1 || clause_index != -1) {
        // scan from ix
        int search_index = (best_clause_index == -1) ? clause_index : best_clause_index;
        WhereClause& this_clause = where_clauses -> clauses[search_index];
        int use_index = get_column_index(table_index, this_clause.l_col.column_name);
        BufType search_key = new unsigned int [1];
        *(int *) search_key = atoi(this_clause.r_val.value.c_str());
        index_handlers[use_index] -> OpenScan(search_key, (CompOp) this_clause.operand);

        while (1) {
            bool still_have = true;
            int pid, sid;
            if (need_next(this_clause.operand)) {
                still_have = index_handlers[use_index] -> GetNextRecord(pid, sid);
            } else {
                still_have = index_handlers[use_index] -> GetPrevRecord(pid, sid);
            }
            if (!still_have) break;
            BufType data = new unsigned int [sm -> tables[table_index].recordSize >> 2];
            rm_handle -> GetRecord(pid, sid, data);
            int match_res = match_record(data, table_index, where_clauses -> clauses, search_index);
            if (match_res == 2) {
                delete [] data;
                break;
            } else if (match_res == 1) {
                delete [] data;
            } else {
                delete_data.push_back(data);
                delete_pos.push_back(std::make_pair(pid, sid));
            }
        }
        delete [] search_key;
    } else {
        // scan from rm
        RM_FileScan *rm_scan = new RM_FileScan(fm, bpm);
        bool at_least_one = rm_scan -> OpenScan(rm_handle, 0, 0, NO_OP, NULL);
        int pid, sid;
        if (at_least_one) {
            while (1) {
                BufType data = new unsigned int [sm -> tables[table_index].recordSize >> 2];
                if (!rm_scan -> GetNextRecord(pid, sid, data)) {
                    delete [] data;
                    break;
                }
                int match_res = match_record(data, table_index, where_clauses -> clauses, -1);
                if (match_res == 2) {
                    delete [] data;
                    break;
                } else if (match_res == 1) {
                    delete [] data;
                } else {
                    delete_data.push_back(data);
                    delete_pos.push_back(std::make_pair(pid, sid));
                }
            }
        }
        delete rm_scan;
    }

    // delete all from delete data
    if (!error) {
        bool update = true;
        if (update_primary) {
            if (delete_data.size() > 1) {
                update = false;
                std::cerr << "Error: Update more than one primary key to same value!" << std::endl;
            } else if (delete_data.size() == 1) {
                index_handlers[primary_index] -> DeleteEntry(delete_data[0] + this_table.attrs[primary_index].offset, delete_pos[0].first, delete_pos[0].second);
                for (auto &set_clause: update_op -> set_clauses) {
                    if (set_clause.column_name != this_table.attrs[primary_index].attrName)
                        continue;
                    if (index_handlers[primary_index] -> HasRecord(&new_primary)) {
                        std::cerr << "Error: Update set value conflicts with current primary!" << std::endl;
                        update = false;
                    }
                    break;
                }
                if (update && *(delete_data[0] + this_table.attrs[primary_index].offset) != new_primary) {
                    for (int i = 0; i < foreign_handlers.size(); i++) {
                        if (foreign_handlers[i] -> HasRecord(delete_data[0] + this_table.attrs[primary_index].offset)) {
                            update = false;
                            std::cerr << "Error: foreign key constraint not matched, cannot update!" << std::endl;
                            break;
                        }
                    }
                }
                index_handlers[primary_index] -> InsertEntry(delete_data[0] + this_table.attrs[primary_index].offset, delete_pos[0].first, delete_pos[0].second);
            }
        } 
        if (update) {
            for (int i = 0; i < delete_data.size(); i++) {
                for (int j = 0; j < this_table.attrs.size(); j++) {
                    if (this_table.attrs[j].isIndex) {
                        index_handlers[j] -> DeleteEntry(delete_data[i] + this_table.attrs[j].offset, delete_pos[i].first, delete_pos[i].second);
                    }
                }
                unsigned long long *bitmap = (unsigned long long *) delete_data[i];
                for (auto &set_clause: update_op -> set_clauses) {
                    int col_index = get_column_index(table_index, set_clause.column_name);
                    if (set_clause.update_value.ret_type == INT_TYPE) {
                        *bitmap |= (1ull << col_index);
                        int update_value = atoi(set_clause.update_value.value.c_str());
                        memcpy(delete_data[i] + this_table.attrs[col_index].offset, &update_value, sizeof(int));
                    } else if (set_clause.update_value.ret_type == FLOAT_TYPE) {
                        *bitmap |= (1ull << col_index);
                        float update_value = atof(set_clause.update_value.value.c_str());
                        memcpy(delete_data[i] + this_table.attrs[col_index].offset, &update_value, sizeof(float));
                    } else if (set_clause.update_value.ret_type == STRING_TYPE) {
                        *bitmap |= (1ull << col_index);
                        memset(delete_data[i] + this_table.attrs[col_index].offset, 0, sizeof(char) * this_table.attrs[col_index].attrLength);
                        memcpy(delete_data[i] + this_table.attrs[col_index].offset, set_clause.update_value.value.c_str(), sizeof(char) * set_clause.update_value.value.length());
                    } else if (set_clause.update_value.ret_type == NULL_TYPE) {
                        *bitmap &= (~(1ull << col_index));
                        memset(delete_data[i] + this_table.attrs[col_index].offset, 0, sizeof(char) * this_table.attrs[col_index].attrLength);
                    }
                }
                for (int j = 0; j < this_table.attrs.size(); j++) {
                    if (this_table.attrs[j].isIndex) {
                        index_handlers[j] -> InsertEntry(delete_data[i] + this_table.attrs[j].offset, delete_pos[i].first, delete_pos[i].second);
                    }
                }
                rm_handle -> UpdateRecord(delete_pos[i].first, delete_pos[i].second, delete_data[i]);
            }
        }
    }

    // clean up, close all indexes, delete data from vector
    delete rm_handle;
    for (int i = 0; i < this_table.attrNum; i++) {
        if (this_table.attrs[i].isIndex) {
            ix -> CloseIndex(ix_fid[i]);
            delete index_handlers[i];
        }
    }
    for (int i = 0; i < foreign_handlers.size(); i++) {
        ix -> CloseIndex(foreign_table_fids[i]);
        delete foreign_handlers[i];
    }
    // clean up data from vector
    for (int i = 0; i < delete_data.size(); i++) {
        delete [] delete_data[i];
    }
}


bool ql_manager::validate_column(Column& col, std::vector<std::string>& table_names){
    bool valid_selector = true;
    if(col.tablename == ""){ // need to see which table
        int total = 0;
        for (auto& table_name : table_names){
            if(table_has_column(table_name, col.column_name)) total += 1;
        }
        if(total != 1){
            valid_selector = false;
        } else {
            for (auto& table_name : table_names){
                if(table_has_column(table_name, col.column_name)) col.tablename = table_name;
            }
        }
    } else {
        bool in_tables;
        for (auto& table_name : table_names){
            if(table_name == col.column_name) in_tables = true;
        }
        if(!in_tables){
            valid_selector = false;
        }
        if(!table_has_column(col.tablename, col.column_name)) {
            valid_selector = false;
        }
    }
    return valid_selector;
}

bool ql_manager::validate_where_clause(WhereClauses* where_clauses, std::vector<std::string>& table_names){
    bool valid_clauses = true;
    for(auto& clause: where_clauses->clauses){
        // check lcol exist and in given tables
        bool valid_lcol = validate_column(clause.l_col, table_names);
        // if rcol is there, check as well
        bool valid_rcol = true;
        if(clause.is_operand && clause.use_r_col) valid_rcol = validate_column(clause.r_col, table_names);
        if(!valid_lcol || !valid_rcol){
            fprintf(stderr, "not valid coloumn of clause\n");
            valid_clauses = false;
            break;
        }
        // if operand check type
        if(clause.is_operand){
            int l_table_index = get_table_index(clause.l_col.tablename);
            int l_col_index = get_column_index(l_table_index, clause.l_col.column_name);
            if(clause.use_r_col){
                int r_table_index = get_table_index(clause.r_col.tablename);
                int r_col_index = get_column_index(r_table_index, clause.r_col.column_name);
                if (sm->tables[l_table_index].attrs[l_col_index].attrType != sm->tables[r_table_index].attrs[r_col_index].attrType) {
                    valid_clauses = false;
                    fprintf(stderr, "operand of clause has mismatched type!\n");
                    break;
                }
            } else {
                if(!match_type(sm->tables[l_table_index].attrs[l_col_index].attrType, clause.r_val.ret_type)){
                    valid_clauses = false;
                    fprintf(stderr, "operand of clause has mismatched type!\n");
                    break;
                }
            }
        }
    }
    return valid_clauses;
}

void ql_manager::display_result(std::vector<int>& attrID, std::vector<BufType>& buffers, std::string& table_name){
    int table_index = get_table_index(table_name);
    int total = 0;
    if (buffers.size()==0){
        printf("total : 0 result(s)\n");
        return;
    }
    bool bar_printed = false;
    for(int i = 0 ;i < attrID.size();i++){
        if(attrID[i]==-1){
            // printf("| COUNT(*) ");
            continue;
        } else {
            printf ("| %s ", sm->tables[table_index].attrs[attrID[i]].attrName.c_str());
            bar_printed = true;
        }
    }
    if(bar_printed) printf("|\n");
    for (auto data: buffers){
        total ++;
        bool printed = false;
        for(auto col_index: attrID){
            if (col_index == -1){
                // printf("COUNT(*): %lld,", buffers.size());
                continue;
            }
            printf ("| %s ", data_to_string(data, table_index, col_index).c_str());
            printed = true;
        }
        if(printed) printf("|\n");
    }
    printf("total : %d result(s)\n", total);
}

void ql_manager::Select(SelectOp* select_op){
    // validate the table names
    for (auto table_name : select_op -> table_names){
        if(get_table_index(table_name) == -1){
            fprintf(stderr, "Error: no such table %s\n", table_name.c_str());
            return;
        }
    }

    // validate selectors. all of them are in the tables
    // if table name is not specified, only one table can be matched!
    bool valid_selector = true;
    std::vector<int> attr1ID;
    std::vector<int> attr2ID;

    for (auto& selector : select_op -> selectors){
        if(selector.is_count) {
            attr1ID.push_back(-1);
            continue;
        }
        bool valid = validate_column(selector.col, select_op->table_names);
        if (!valid) valid_selector = false;
        else {
            if(selector.col.tablename == select_op->table_names[0])
                attr1ID.push_back(get_column_index(get_table_index(selector.col.tablename),selector.col.column_name));
            else
                attr2ID.push_back(get_column_index(get_table_index(selector.col.tablename),selector.col.column_name));
        }
    }
    if (!valid_selector){
        fprintf(stderr, "error when parsing the selectors, some of them are wrong\n");
        return;
    }

    // validate the where clauses, all of them are in the tables
    WhereClauses* where_clauses;
    if (select_op->info){
        where_clauses = dynamic_cast<WhereClauses*>(select_op -> info);
    }
    bool valid_clauses = validate_where_clause(where_clauses, select_op -> table_names);
    if(!valid_clauses){
        fprintf(stderr, "error when parsing the where clauses, some of them are wrong\n");
        return;
    }

    // do the select
    if(select_op -> table_names.size() == 1) {
        std::vector<BufType> select_res;
        WhereClauses* info = dynamic_cast<WhereClauses*>(select_op -> info);
        Select_one_table(info->clauses, select_res, select_op->table_names[0]);
        display_result(attr1ID, select_res, select_op->table_names[0]);    
        for (auto data: select_res){
            delete[] data;
        }
    } else if (select_op -> table_names.size() == 2) {
        std::vector<BufType> select_res_1;
        WhereClauses* info = dynamic_cast<WhereClauses*>(select_op -> info);
        std::vector<BufType> select_res_2;
        std::vector<WhereClause> attr1Clause;
        std::vector<WhereClause> attr2Clause;
        std::vector<WhereClause> shareClause;
        for (auto& clause: info->clauses){
            if(clause.use_r_col && clause.is_operand){
                // see if shared
                if(clause.r_col.tablename != clause.l_col.tablename){
                    shareClause.push_back(clause);
                } else if (clause.l_col.tablename == select_op->table_names[0]){
                    attr1Clause.push_back(clause);
                } else {
                    attr2Clause.push_back(clause);
                }
            } else if (clause.l_col.tablename == select_op->table_names[0]){
                attr1Clause.push_back(clause);
            } else {
                attr2Clause.push_back(clause);
            }
        }
        Select_one_table(attr1Clause, select_res_1, select_op->table_names[0]);
        Select_one_table(attr2Clause, select_res_2, select_op->table_names[1]);
        
        Select_two_table(select_op, shareClause, select_res_1, select_res_2 , attr1ID, attr2ID);
        for (auto data: select_res_1){
            delete[] data;
        }
        for (auto data: select_res_2){
            delete[] data;
        }
    } else {
        fprintf(stderr , "selecting from more than two tables, not implemented!\n");
    }
}

int ql_manager::match_record(BufType data, int table_index, std::vector<WhereClause>& clauses, int clause_index){
    // clause_index : this is the clause where we use the optimize
    // if we do not use the optimize , it should be given a (-1)
    // for return value:
    // if 0 is returned, it is a match
    // if 1 is returned, it is a not match
    // if 2 is returned, it is a index_clause not match (which means we can abort selecting)
    const TableMeta& this_table_meta = sm->tables[table_index];
    bool ok = true;

	unsigned long long *bitmap = (unsigned long long*)data;
    for (int i = 0; i < clauses.size(); i++){
        auto& clause = clauses[i];
        int col_index = get_column_index(table_index, clause.l_col.column_name);
        int col_offset = this_table_meta.attrs[col_index].offset;
        // get null info from null map
        unsigned long long if_null = (*bitmap) & (1ull << col_index);
        bool is_null_col = (if_null == 0);
        bool matched = true;
        BufType l_col_value = data + col_offset;
        if(clause.is_operand){
            BufType r_col_value = nullptr;
            bool rcol_is_null = false;
            if(clause.use_r_col){
                rcol_is_null = (*bitmap) & (1ull << get_column_index(table_index, clause.r_col.column_name));
                r_col_value = data + this_table_meta.attrs[get_column_index(table_index, clause.r_col.column_name)].offset;
            } else {
                r_col_value = new unsigned int[this_table_meta.attrs[col_index].attrLength >> 2];
                if (clause.r_val.ret_type == INT_TYPE) {
                    unsigned int* r_val =  (unsigned int*)r_col_value;
                    *r_val = atoi(clause.r_val.value.c_str());
                } else if(clause.r_val.ret_type == FLOAT_TYPE) {
                    float* r_val =  (float*)r_col_value;
                    *r_val = atof(clause.r_val.value.c_str());
                }
            }
            // compare the left and right
            if(this_table_meta.attrs[col_index].attrType == INT_ATTRTYPE){
                unsigned int* left = (unsigned int*)l_col_value;
                unsigned int* right = (unsigned int*)r_col_value;
                matched = comp_op(*left, *right, clause.operand);
            } else if(this_table_meta.attrs[col_index].attrType == FLOAT_ATTRTYPE){
                float* left = (float*)l_col_value;
                float* right = (float*)r_col_value;
                matched = comp_op(*left, *right, clause.operand);
            } else if (this_table_meta.attrs[col_index].attrType == STRING_ATTRTYPE){
                std::string l_col_str((const char*)l_col_value,(std::size_t)this_table_meta.attrs[col_index].original_attrLength);
                std::string r_col_str;
                if (clause.use_r_col){
                    std::string temp((const char*)r_col_value,(std::size_t)this_table_meta.attrs[get_column_index(table_index, clause.r_col.column_name)].original_attrLength);
                    int zero_pos = temp.find('\0');
                    if(zero_pos != -1) temp = temp.substr(0,zero_pos);
                    r_col_str = temp;
                } else {
                    r_col_str = clause.r_val.value;
                }
                int zero_pos = l_col_str.find('\0');
                if (zero_pos != -1)
                    l_col_str = l_col_str.substr(0, zero_pos);
                matched = comp_op(l_col_str, r_col_str, clause.operand);
            }
            if(is_null_col || rcol_is_null) matched = false;
            if(!clause.use_r_col) delete[] r_col_value;
        } else if(clause.in_value_list){
            matched = false;
            for (auto& value: clause.value_list.values){
                if(this_table_meta.attrs[col_index].attrType == INT_ATTRTYPE && value.ret_type == INT_TYPE){
                    int l_col_int = *(int*)l_col_value;
                    matched = (l_col_int == atoi(value.value.c_str()));
                } else if (this_table_meta.attrs[col_index].attrType == FLOAT_ATTRTYPE && value.ret_type == FLOAT_TYPE){
                    float l_col_double = *(float*)l_col_value;
                    matched = (l_col_double == atof(value.value.c_str()));
                } else if (this_table_meta.attrs[col_index].attrType == STRING_ATTRTYPE && value.ret_type == STRING_TYPE){
                    std::string l_col_str((const char*)l_col_value,(std::size_t)this_table_meta.attrs[col_index].original_attrLength);
                    int zero_pos = l_col_str.find('\0');
                    if (zero_pos != -1) l_col_str = l_col_str.substr(0,zero_pos);
                    matched = (l_col_str == value.value);
                }
                if(matched) break;
            }
            matched = matched & (!is_null_col); // if null column, then not equal or in
        } else if(clause.is_Null){
            unsigned long long if_null = (*bitmap) & (1ull << col_index);
            if(if_null != 0) matched = false;
        } else if(clause.is_not_Null){
            unsigned long long if_null = (*bitmap) & (1ull << col_index);
            if(if_null == 0) matched = false;
        }
        if(!matched){
            ok = false;
            if (i == clause_index) return 2;
        }
    }
    if(ok) return 0;
    else return 1;
}

void ql_manager::Select_one_table(std::vector<WhereClause>& clauses, std::vector<BufType>& select_result, std::string& table_name){
    // loop in clause, find the first condition using index and equal
    // if can not find, find the first condition using index
    // if both above failed scan the record
    int best_clause_index = -1;
    int clause_index = -1;
    for (int i = 0; i < clauses.size(); i++){
        WhereClause& this_clause = clauses[i];
        if(this_clause.is_operand && this_clause.operand==EQUAL 
            && is_index_column(this_clause.l_col.tablename, this_clause.l_col.column_name) && !this_clause.use_r_col){
                best_clause_index = i;
                break;
            }
        if(this_clause.is_operand && this_clause.operand!=NOT_EQUAL 
            && is_index_column(this_clause.l_col.tablename, this_clause.l_col.column_name) && !this_clause.use_r_col){
                clause_index = i;
            }
    }
	RM_FileScan *filescan = new RM_FileScan(fm, bpm);
    IX_IndexHandle *index_handle = nullptr;
    int idx_id;
    int fid = sm -> table_to_fid[table_name];
    RM_FileHandle * rm_handle = new RM_FileHandle(fm, bpm, fid);
    int table_index = get_table_index(table_name);

    if(best_clause_index != -1 || clause_index != -1){
        // search index is the index of the clauses that we used to optimize search
        int search_index = (best_clause_index == -1) ? clause_index : best_clause_index;
        WhereClause& this_clause = clauses[search_index];
        ix->OpenIndex(this_clause.l_col.tablename.c_str(), this_clause.l_col.column_name.c_str(), idx_id);
        index_handle = new IX_IndexHandle(fm,bpm,idx_id);
        BufType search_key = new unsigned int [1];
        *(int*)search_key = atoi(this_clause.r_val.value.c_str());
        bool at_least_one = index_handle -> OpenScan(search_key, (CompOp)this_clause.operand);

        while(1){
            if(!at_least_one) break;
            bool still_have = true;
            // begin searching
            int pid, sid;
            if(need_next(this_clause.operand)){
                still_have = index_handle -> GetNextRecord(pid,sid);
            } else {
                still_have = index_handle -> GetPrevRecord(pid,sid);
            }
            if(!still_have) break;
            BufType data = new unsigned int [sm->tables[table_index].recordSize >> 2];
            rm_handle -> GetRecord(pid, sid, data);
            int match_res = match_record(data,table_index,clauses,search_index);
            if(match_res == 2){
                delete[] data;
                break;
            }
            else if(match_res == 1) delete[]data;
            else select_result.push_back(data);
            // if (!still_have) break;
        }
        ix->CloseIndex(idx_id);
        delete[] search_key;
        delete index_handle;
    } else { // do the scan anyway
        bool at_least_one = filescan -> OpenScan(rm_handle, sm->tables[table_index].recordSize,0,NO_OP, NULL);
        int pid, sid;
        if(at_least_one){
            while(1){
                BufType data = new unsigned int [sm->tables[table_index].recordSize >> 2];
                if(!filescan->GetNextRecord(pid,sid,data)){
                    delete[]data;
                    break;
                }
                int match_res = match_record(data,table_index,clauses,-1);
                if(match_res == 2){
                    delete[]data;
                    break;
                }
                else if(match_res == 1) {
                    delete[]data;
                } else {
                    select_result.push_back(data);
                }
            }
        }
    }
    delete rm_handle;
    delete filescan;
}

std::string ql_manager::data_to_string(BufType data, int table_index, int col_index){
    unsigned long long *bitmap = (unsigned long long*)data;
    bool if_null = (*bitmap & (1ull << col_index)) == 0;
    if (if_null) return "NULL";
    int offset = sm->tables[table_index].attrs[col_index].offset;
    if(sm->tables[table_index].attrs[col_index].attrType == STRING_ATTRTYPE){
        std::string l_col_str((const char*)(data+offset),(std::size_t)sm->tables[table_index].attrs[col_index].original_attrLength);
        int zero_pos = l_col_str.find('\0');
        if (zero_pos != -1) l_col_str = l_col_str.substr(0,zero_pos);
        return l_col_str;
    } else if (sm->tables[table_index].attrs[col_index].attrType == INT_ATTRTYPE){
        unsigned int* int_val= (unsigned int *)(data+offset);
        return std::to_string(*int_val);
    } else if (sm->tables[table_index].attrs[col_index].attrType == FLOAT_ATTRTYPE){
        float* int_val= (float *)(data+offset);
        return std::to_string(*int_val);
    } else {
        return "";
    }
}

void ql_manager::Select_two_table(SelectOp* select_op, std::vector<WhereClause>& clauses, std::vector<BufType>& select_res1,
    std::vector<BufType>& select_res2, std::vector<int>& attr1ID, std::vector<int>& attr2ID){
    int table1_index = get_table_index(select_op -> table_names[0]);
    int table2_index = get_table_index(select_op -> table_names[1]);
    bool bar_printed = false;
    for(int i = 0 ;i < attr1ID.size();i++){
        if(attr1ID[i]==-1){
            // printf("| COUNT(*) ");
            continue;
        } else {
            printf ("| %s ", sm->tables[table1_index].attrs[attr1ID[i]].attrName.c_str());
            bar_printed = true;
        }
    }
    for(int i = 0 ;i < attr2ID.size();i++){
        printf ("| %s ", sm->tables[table2_index].attrs[attr2ID[i]].attrName.c_str());
        bar_printed = true;
    }
    if(bar_printed) printf("|\n");
    int total_count = 0;
    for (auto data1: select_res1){ // first table
        for (auto data2: select_res2){ // second table
            // check if meet share_clauses
            // if meet then output
            bool ok = true;
            for (int i = 0; i < clauses.size(); i++){
                auto& clause = clauses[i];
                int l_table_index = (clause.l_col.tablename == select_op -> table_names[0]) ? table1_index : table2_index;
                int l_col_index = get_column_index(l_table_index, clause.l_col.column_name);
                int l_col_offset = sm->tables[l_table_index].attrs[l_col_index].offset;
                BufType l_data = (clause.l_col.tablename == select_op -> table_names[0]) ? data1 : data2;
                unsigned long long* l_bitmap = (unsigned long long*)l_data;
                bool l_null_col = ( ((*l_bitmap) & (1ull << l_col_index))== 0);

                int r_table_index = (clause.r_col.tablename == select_op -> table_names[0]) ? table1_index : table2_index;
                int r_col_index = get_column_index(r_table_index, clause.r_col.column_name);
                int r_col_offset = sm->tables[r_table_index].attrs[r_col_index].offset;
                BufType r_data = (clause.r_col.tablename == select_op -> table_names[0]) ? data1 : data2;
                unsigned long long* r_bitmap = (unsigned long long*)r_data;
                bool r_null_col = ( ((*r_bitmap) & (1ull << r_col_index))== 0);

                // get null info from null map
                if(clause.is_operand){
                    // compare the left and right
                    if(sm->tables[r_table_index].attrs[r_col_index].attrType == INT_ATTRTYPE){
                        unsigned int* left = (unsigned int*)(l_data+l_col_offset);
                        unsigned int* right = (unsigned int*)(r_data+r_col_offset);
                        ok = comp_op(*left, *right, clause.operand);
                    } else if(sm->tables[r_table_index].attrs[r_col_index].attrType == FLOAT_ATTRTYPE){
                        float* left = (float*)(l_data+l_col_offset);
                        float* right = (float*)(r_data+r_col_offset);
                        ok = comp_op(*left, *right, clause.operand);
                    } else if(sm->tables[r_table_index].attrs[r_col_index].attrType == STRING_ATTRTYPE){
                        std::string l_col_str((const char*)(l_data+l_col_offset),(std::size_t)sm->tables[l_table_index].attrs[l_col_index].original_attrLength);
                        int l_zero_pos = l_col_str.find('\0');
                        l_col_str = l_col_str.substr(0, l_zero_pos);
                        std::string r_col_str((const char*)(r_data+r_col_offset),(std::size_t)sm->tables[r_table_index].attrs[r_col_index].original_attrLength);
                        int r_zero_pos = r_col_str.find('\0');
                        r_col_str = r_col_str.substr(0, r_zero_pos);
                        ok = comp_op(l_col_str, r_col_str, clause.operand);
                    }
                    if(l_null_col || r_null_col) ok = false;
                }
                if(!ok) break;
            }
            if(ok){
                total_count++;
                // do the printing
                for(int i = 0 ;i < attr1ID.size();i++){
                    if(attr1ID[i]==-1){
                        // printf("|    -     ");
                    } else {
                        printf ("| %s ", data_to_string(data1, table1_index, attr1ID[i]).c_str());
                    }
                }
                for(int i = 0 ;i < attr2ID.size();i++){
                    printf ("| %s ", data_to_string(data2, table2_index, attr2ID[i]).c_str());
                }
                if(bar_printed) printf("|\n");
            }
        }
    }
    printf("total: %d result(s)\n", total_count);
}

void ql_manager::Load(LoadOp* load_op){
    printf("start loading from file\n");
    InsertOp* insert_op = new InsertOp(load_op->table_name);
    InsertInfo* insert_info = new InsertInfo();
    insert_op -> info = insert_info;
    std::string line, word;
    
    std::fstream file(load_op->file_name,ios::in);
    int table_index = get_table_index(load_op->table_name);
    TableMeta& this_table = sm -> tables[table_index];
    if(file.is_open()){
        int record_num = 0;
        while(getline(file, line)){
            stringstream str(line);
            input_attribute attr;
            int col_idx = 0;
            while(getline(str, word, ',')){
                int type_in = this_table.attrs[col_idx].attrType;
                if (type_in == STRING_ATTRTYPE){
                    attr.values.push_back(input_value(word, STRING_TYPE));
                } else if (type_in == INT_ATTRTYPE) {
                    attr.values.push_back(input_value(word,INT_TYPE));
                } else if (type_in == FLOAT_ATTRTYPE){
                    attr.values.push_back(input_value(word,FLOAT_TYPE));
                }
                col_idx++;
            }
            insert_info -> attributes.push_back(attr);
            record_num++;
        }
        printf("%d records are retrieved from file, start inserting\n", record_num);
    } else {
        printf("failed to open file %s, please check\n", load_op -> file_name.c_str());
    }

    Insert(insert_op);
    delete insert_op;
}
