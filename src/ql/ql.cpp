#include "ql.h"
#include <cstring>

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
    if(attr_type == in_type) return true;
    if(attr_type == FLOAT_TYPE && in_type == INT_TYPE) return true;
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

inline bool ql_manager::comp_op(double a, double b, int op){
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

void ql_manager::Insert(InsertOp* insert_op){
    // TODO null bit map
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
    for (int i = 0;i < this_table.attrNum; i++){
        if(this_table.attrs[i].isIndex){
            int idx_fid;
            // TODO see where to close Index
            bool openFlag = ix -> OpenIndex(insert_op->table_name.c_str(), this_table.attrs[i].attrName.c_str(), idx_fid);
            index_handlers[i] = new IX_IndexHandle(fm, bpm, idx_fid);
        } else {
            index_handlers[i] = nullptr;
        }
    }

    for (auto& insert_attribute : insert_info->attributes){
        // do the loop, insert one at a time
        if (insert_attribute.values.size() != this_table.attrNum){
            error_when_insert(insert_attribute, "attribute num not match");
            continue;
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
        if (!type_check) continue;
        // prepare insertion data
        bool insert_success = true;
        BufType data = new unsigned int[this_table.recordSize >> 2];
        for (int i = 0; i < insert_attribute.values.size(); i++){
            // if primary check unique
            // if foreign key, check other exist
            // if not Null check null?
            if(!insert_success) break;
            if (this_table.attrs[i].isPrimary){
                int pk_index = atoi(insert_attribute.values[i].value.c_str());
                // TODO check pk, see if it does not exist
            } else if (this_table.attrs[i].isForeign) {
                int fk_index = atoi(insert_attribute.values[i].value.c_str());
                // TODO check fk, see if it exist
            } else if (this_table.attrs[i].isNotNULL) {
                if(insert_attribute.values[i].ret_type == NULL_TYPE){
                    error_when_insert(insert_attribute, "insert Null into not Null coloumn");
                    insert_success = false;
                    continue;
                }
            }
            if(this_table.attrs[i].attrType == INT_TYPE){
                int inserted_value = atoi(insert_attribute.values[i].value.c_str());
                memcpy(data + this_table.attrs[i].offset, &inserted_value, 4);
            } else if (this_table.attrs[i].attrType == FLOAT_TYPE) {
                double inserted_value = atof(insert_attribute.values[i].value.c_str());
                memcpy(data + this_table.attrs[i].offset, &inserted_value,8);
            } else if (this_table.attrs[i].attrType == STRING_TYPE) {
                // copy the value into buffer, if there is spare space, add \0
                memset(data + this_table.attrs[i].offset, 0, sizeof(char) * this_table.attrs[i].attrLength);
                memcpy(data + this_table.attrs[i].offset, insert_attribute.values[i].value.c_str(), sizeof(char) * insert_attribute.values[i].value.length());
                // when taking out these attributes please add \0 in the end
            }
        }
        
        // insert the record
        int pid, sid;
        bool insert_record_success = rm_handle -> InsertRecord(pid, sid, data);
        if(!insert_record_success) fprintf(stderr, "internal error occured when inserting record\n");
        
        // insert primary key index and all other index in the table
        for(int i = 0; i < insert_attribute.values.size();i++) {
            if(this_table.attrs[i].isIndex){
                index_handlers[i] -> InsertEntry(data+this_table.attrs[i].offset, pid, fid);
            }
        }
        delete[] data;
    }
    for (int i = 0;i < this_table.attrNum; i++){
        if(index_handlers[i]) delete index_handlers[i];
    }
    delete rm_handle;
}

void ql_manager::Delete(DeleteOp* delete_op){
    // TODO
}

void ql_manager::Select(SelectOp* select_op){
    // validate selectors. all of them are in the tables
    // if table name is not specified, only one table can be matched!
    bool valid_selector = true;
    for (auto& selector : select_op -> selectors){
        if(selector.is_count) continue;
        if(selector.col.tablename == ""){ // need to see which table
            int total = 0;
            for (auto& table_name : select_op -> table_names){
                if(table_has_column(table_name, selector.col.column_name)) total += 1;
            }
            if(total != 1){
                valid_selector = false;
                break;
            }
        } else {
            bool in_tables;
            for (auto& table_name : select_op -> table_names){
                if(table_name == selector.col.column_name) in_tables = true;
            }
            if(!in_tables){
                valid_selector = false;
                break;
            }
            if(!table_has_column(selector.col.tablename, selector.col.column_name)) {
                valid_selector = false;
                break;
            }
        }
    }

    // validate the where clauses, all of them are in the tables
    WhereClauses* where_clauses;
    if (select_op->info){
        where_clauses = dynamic_cast<WhereClauses*>(select_op -> info);
    } else {
        where_clauses = new WhereClauses();
    }
    for(auto& clause: where_clauses->clauses){
        // TODO
        // check lcol exist and in given tables
        // if rcol is there, check as well
        // if operand check type
        // fill in the blank column
    }
    if(select_op -> table_names.size() == 1) {
        Select_one_table(select_op);
    } else if (select_op -> table_names.size() == 2) {

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
    // FIXME note the situation of a null value
	unsigned long long *bitmap = (unsigned long long*)data;
    for (int i = 0; i < clauses.size(); i++){
        auto& clause = clauses[i];
        int col_index = get_column_index(table_index, clause.l_col.column_name);
        int col_offset = this_table_meta.attrs[col_index].offset;
        // get null info from null map
        bool matched = true;
        BufType l_col_value = data + col_offset;
        if(clause.is_operand){
            BufType r_col_value = nullptr;
            if(clause.use_r_col){
                r_col_value = data + this_table_meta.attrs[get_column_index(table_index, clause.r_col.column_name)].offset;
            } else {
                r_col_value = new unsigned int[this_table_meta.attrs[col_index].attrLength >> 2];
                if (clause.r_val.ret_type == INT_TYPE) {
                    unsigned int* r_val =  (unsigned int*)r_col_value;
                    *r_val = atoi(clause.r_val.value.c_str());
                } else if(clause.r_val.ret_type == FLOAT_TYPE) {
                    double* r_val =  (double*)r_col_value;
                    *r_val = atof(clause.r_val.value.c_str());
                }
            }
            // compare the left and right
            if(this_table_meta.attrs[col_index].attrType == INT_TYPE){
                unsigned int* left = (unsigned int*)l_col_value;
                unsigned int* right = (unsigned int*)r_col_value;
                matched = comp_op(*left, *right, clause.operand);
            } else if(this_table_meta.attrs[col_index].attrType == FLOAT_TYPE){
                double* left = (double*)l_col_value;
                double* right = (double*)r_col_value;
                matched = comp_op(*left, *right, clause.operand);
            }
            if(!clause.use_r_col) delete[] r_col_value;
        } else if(clause.in_value_list){
            // TODO
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

void ql_manager::Select_one_table(SelectOp* select_op){
    WhereClauses* where_clauses = dynamic_cast<WhereClauses*>(select_op -> info);
    // loop in clause, find the first condition using index and equal
    // if can not find, find the first condition using index
    // if both above failed scan the record
    int best_clause_index = -1;
    int clause_index = -1;
    for (int i = 0; i < where_clauses -> clauses.size(); i++){
        WhereClause& this_clause = where_clauses -> clauses[i];
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
    if(best_clause_index != -1 || clause_index != -1){
        // search index is the index of the clauses that we used to optimize search
        int search_index = (best_clause_index == -1) ? clause_index : best_clause_index;
        WhereClause& this_clause = where_clauses -> clauses[search_index];
        ix->OpenIndex(this_clause.l_col.tablename.c_str(), this_clause.l_col.column_name.c_str(), idx_id);
        index_handle = new IX_IndexHandle(fm,bpm,idx_id);
        int table_index = get_table_index(this_clause.l_col.tablename);
        BufType search_key = new unsigned int [1];
        *(int*)search_key = atoi(this_clause.r_val.value.c_str());
        index_handle -> OpenScan(search_key, (CompOp)this_clause.operand);

        int fid = sm -> table_to_fid[this_clause.l_col.tablename];
        RM_FileHandle * rm_handle = new RM_FileHandle(fm, bpm, fid);

        while(1){
            bool still_have = true;
            // begin searching
            int pid, sid;
            if(need_next(this_clause.operand)){
                still_have = index_handle -> GetNextRecord(pid,sid);
            } else {
                still_have = index_handle -> GetPrevRecord(pid,sid);
            }
            BufType data = new unsigned int [sm->tables[table_index].recordSize >> 2];
            rm_handle -> GetRecord(pid, sid, data);


            // bool has_next = index_handle -> 
        }
        ix->CloseIndex(idx_id);
        delete[] search_key;
    } else { // do the scan anyway

    }
}
