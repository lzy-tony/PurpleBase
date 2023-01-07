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
            if(!insert_success) break;
            if (this_table.attrs[i].isPrimary){
                int pk_index = atoi(insert_attribute.values[i].value.c_str());
                if(index_handlers[i] -> HasRecord(&pk_index)){
                    error_when_insert(insert_attribute, "unique constraint of primary key violated");
                    insert_success = false;
                    continue;
                }
            } else if (this_table.attrs[i].isForeign) {
                int fk_index = atoi(insert_attribute.values[i].value.c_str());
                if(!foreign_index_handlers[i] -> HasRecord(&fk_index)){
                    error_when_insert(insert_attribute, "referring to a foreign key that doesn't exist");
                    insert_success = false;
                    continue;
                }
            } else if (this_table.attrs[i].isNotNULL) {
                if(insert_attribute.values[i].ret_type == NULL_TYPE){
                    error_when_insert(insert_attribute, "insert Null into not Null coloumn");
                    insert_success = false;
                    continue;
                }
            }
            // FIXME check string len here, make sure it does not exceed!
            if(this_table.attrs[i].attrType == INT_ATTRTYPE){
                *bitmap |= (1ull<<i);
                int inserted_value = atoi(insert_attribute.values[i].value.c_str());
                memcpy(data + this_table.attrs[i].offset, &inserted_value, 4);
            } else if (this_table.attrs[i].attrType == FLOAT_ATTRTYPE) {
                *bitmap |= (1ull<<i);
                double inserted_value = atof(insert_attribute.values[i].value.c_str());
                memcpy(data + this_table.attrs[i].offset, &inserted_value,8);
            } else if (this_table.attrs[i].attrType == STRING_ATTRTYPE) {
                *bitmap |= (1ull<<i);
                // copy the value into buffer, if there is spare space, add \0
                memset(data + this_table.attrs[i].offset, 0, sizeof(char) * this_table.attrs[i].attrLength);
                memcpy(data + this_table.attrs[i].offset, insert_attribute.values[i].value.c_str(), sizeof(char) * insert_attribute.values[i].value.length());
                // when taking out these attributes please add \0 in the end
            }
        }
        if(!insert_success){
            delete[] data;
            break;
        }

        // insert the record
        int pid, sid;
        bool insert_record_success = rm_handle -> InsertRecord(pid, sid, data);
        std::cout <<"before create scan"<<std::endl;
        RM_FileScan* rm_scan = new RM_FileScan(fm,bpm);
        bool open_scan_success = rm_scan -> OpenScan(rm_handle, 0, 0, NO_OP, NULL);
        if(!open_scan_success){
            std::cout << "open scan fail" << std::endl;
            exit(1);
        }
        std::cout << this_table.recordSize << std::endl;
        BufType data_get = new unsigned int[this_table.recordSize >> 2];
        std::cout << "new success" << std::endl;
        int pid_get, sid_get;
        bool scanned = rm_scan -> GetNextRecord(pid_get,sid_get,data_get);
        std::cout << "scan success" << std::endl;
        if(!scanned){
            std::cout << "not scanned" << std::endl;
            exit(1);
        } else {
            for (int i = 0; i < (this_table.recordSize >> 2);i++){
                printf("%x ",*(unsigned int*)(data+i));
            }
            printf("\n");
            for (int i = 0; i < (this_table.recordSize >> 2);i++){
                printf("%x ",*(unsigned int*)(data_get+i));
            }
            printf("\n");
            delete[] data_get;
            delete rm_scan;
        }
        if(!insert_record_success) fprintf(stderr, "internal error occured when inserting record\n");
        
        // insert primary key index and all other index in the table
        for(int i = 0; i < insert_attribute.values.size();i++) {
            if(this_table.attrs[i].isIndex){
                index_handlers[i] -> InsertEntry(data+this_table.attrs[i].offset, pid, fid);
            }
        }
        delete[] data;
        std::cout << "delete success" << std::endl;
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
    // TODO add Delete
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
    for (auto data: buffers){
        unsigned long long *bitmap = (unsigned long long*)data;
        for(auto col_index: attrID){
            unsigned long long if_null = (*bitmap) & (1ull << col_index);
            printf("%s : ", sm->tables[table_index].attrs[col_index].attrName.c_str());
            if (if_null == 0){
                printf("NULL,");
            } else if (sm->tables[table_index].attrs[col_index].attrType == INT_ATTRTYPE){
                unsigned int * val = (unsigned int*)(data+sm->tables[table_index].attrs[col_index].offset);
                printf("%d ,",*val);
            } else if (sm->tables[table_index].attrs[col_index].attrType == FLOAT_ATTRTYPE){
                double * val = (double*)(data+sm->tables[table_index].attrs[col_index].offset);
                printf("%.5lf",*val);
            } else if (sm->tables[table_index].attrs[col_index].attrType == STRING_ATTRTYPE){
                std::string val((const char*)(data+sm->tables[table_index].attrs[col_index].offset),
                    (std::size_t)sm->tables[table_index].attrs[col_index].original_attrLength);
                printf("%s ,",val.c_str());
            }
            printf("\n");
        }
    }
}

void ql_manager::Select(SelectOp* select_op){
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
        if(attr1ID.size()==1 && attr1ID[0]==-1){
            // FIXME print count(*) more beautiful
            printf("total count: %lld\n", (long long int)select_res.size());
        } else {
            display_result(attr1ID, select_res, select_op->table_names[0]);
        }
        for (auto data: select_res){
            delete[] data;
        }
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
                    double* r_val =  (double*)r_col_value;
                    *r_val = atof(clause.r_val.value.c_str());
                }
            }
            // compare the left and right
            if(this_table_meta.attrs[col_index].attrType == INT_ATTRTYPE){
                unsigned int* left = (unsigned int*)l_col_value;
                unsigned int* right = (unsigned int*)r_col_value;
                matched = comp_op(*left, *right, clause.operand);
            } else if(this_table_meta.attrs[col_index].attrType == FLOAT_ATTRTYPE){
                double* left = (double*)l_col_value;
                double* right = (double*)r_col_value;
                matched = comp_op(*left, *right, clause.operand);
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
                    double l_col_double = *(double*)l_col_value;
                    matched = (l_col_double == atof(value.value.c_str()));
                } else if (this_table_meta.attrs[col_index].attrType == STRING_ATTRTYPE && value.ret_type == STRING_TYPE){
                    std::string l_col_str((const char*)l_col_value,(std::size_t)this_table_meta.attrs[col_index].original_attrLength);
                    matched = (l_col_str == value.value);
                }
                if(matched) break;
            }
            matched = matched & is_null_col; // if null column, then not equal or in
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
    std::cout << fid << std::endl;
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
