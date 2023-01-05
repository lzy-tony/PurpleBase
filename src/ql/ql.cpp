#include "ql.h"

inline int ql_manager::get_table_index(std::string& table_name){
    for (int i = 0; i < sm->tables.size(); i++){
        if(sm->tables[i].tableName == table_name){
            return i;
        }
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
        BufType data = new unsigned int[this_table.recordSize >> 2];
    }
}