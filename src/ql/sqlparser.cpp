#include "sqlparser.h"
#include <iostream>

std::string error_parse_where = "in where clauses";
std::string error_parse_create = "in create table fields";
std::string error_parse_set_clause = "in set clauses";

OpBase* SQLParser::parse(std::string& _input){
    OpBase* ret_val;
    if (_input[_input.length()-1] != ';') {
        std::string err = "the last word. Did not end with ;";
        ret_val = new ErrorOp(_input, err);
        return ret_val;
    }
    input = _input.substr(0,_input.length()-1);
    pos = 0;
    std::string word_now = get_input_word();
    if (word_now == "NULL" or is_comment(word_now)){ // do nothing

    } else if (word_now == "SHOW"){ // show table or show database
        word_now = get_input_word();
        if (word_now == "DATABASES") {
            ret_val = new ShowDatabaseOp();
        } else if (word_now == "TABLES") {
            ret_val = new ShowTablesOp();
        } else {
            ret_val = new ErrorOp(input, word_now);
        }
    } else if (word_now == "USE") { // use db;
        word_now = get_input_word();
        ret_val = new UseDataBaseOp(word_now);
    } else if (word_now == "CREATE"){
        word_now = get_input_word();
        if (word_now == "DATABASE"){ // create database
            word_now = get_input_word();
            if (word_now=="\0"){
                std::string err = "end, no given database name";
                ret_val = new ErrorOp(input, err);
            } else {
                ret_val = new CreateDatabaseOp(word_now);
            }
        } else if (word_now == "TABLE"){ // create table
            std::string tableName = get_input_word();
            TableInfo* tableInfo = new TableInfo(tableName);
            bool success = read_create_table(tableInfo);
            if (!success) {
                ret_val = new ErrorOp(input, error_parse_create);
            } else {
                ret_val = new CreateTableOp();
                ret_val -> info = tableInfo;
            }
        } else { // raise an error
            ret_val = new ErrorOp(input, word_now);
        }
    } else if (word_now == "DROP") {
        word_now = get_input_word();
        if (word_now == "DATABASE"){ // drop database
            std::string database_identifier = get_input_word();
            if (database_identifier=="\0"){
                std::string err = "end, no given database name";
                ret_val = new ErrorOp(input, err);
            } else {
                ret_val = new DropDatabaseOp(database_identifier);
            }
        } else if (word_now == "TABLE"){ // drop table
            std::string table_identifier = get_input_word();
            if (table_identifier=="\0"){
                std::string err = "end, no given table name";
                ret_val = new ErrorOp(input, err);
            } else {
                ret_val = new DropTableOp(table_identifier);
            }
        } else { // raise an error
            ret_val = new ErrorOp(input, word_now);
        }
    } else if (word_now == "DESC") {
        word_now = get_input_word();
        ret_val = new DESCOp(word_now);    
    } else if (word_now == "INSERT") {
        word_now = get_input_word();
        if (word_now != "INTO"){
            ret_val = new ErrorOp(input, word_now);
            return ret_val;
        }
        word_now = get_input_word();
        ret_val = new InsertOp(word_now);
        InsertInfo* insert_info = new InsertInfo();
        word_now = get_input_word();
        if (word_now != "VALUES"){
            ret_val = new ErrorOp(input, word_now);
            return ret_val;
        }
        ret_val -> info = insert_info;
        read_value_list(insert_info);
    } else if (word_now == "DELETE") {
        word_now = get_input_word();
        if (word_now != "FROM"){
            ret_val = new ErrorOp(input, word_now);
            return ret_val;
        }
        std::string table_name = get_input_word();
        word_now = get_input_word();
        if (word_now != "WHERE"){
            ret_val = new ErrorOp(input, word_now);
            return ret_val;
        }
        WhereClauses* wc = new WhereClauses();
        bool success = read_where_clauses(wc);
        if(!success){
            ret_val = new ErrorOp(input, error_parse_where);
            delete wc;
        } else {
            ret_val = new DeleteOp(table_name);
            ret_val->info = wc;
        }
    } else if (word_now == "UPDATE") {
        std::string table_name = get_input_word();
        word_now = get_input_word();
        if(word_now != "SET"){
            ret_val = new ErrorOp(input, word_now);
            return ret_val;
        }
        UpdateOp* update_op = new UpdateOp(table_name);
        bool success = read_set_clauses(update_op);
        if(!success){
            ret_val = new ErrorOp(input, error_parse_set_clause);
            delete update_op;
            return ret_val;
        }
        WhereClauses* update_cond = new WhereClauses();
        success = read_where_clauses(update_cond);
        if(!success){
            ret_val = new ErrorOp(input, error_parse_where);
            delete update_op;
            delete update_cond;
            return ret_val;
        }
        update_op -> info = update_cond;
        ret_val = update_op;
    } else if (word_now == "SELECT") {
        SelectOp* select_op = new SelectOp();
        while(1){
            word_now = get_input_word();
            Selector sel;
            if (word_now == "COUNT"){
                sel.is_count = true;
                get_input_word();
                get_input_word();
                get_input_word();
            } else if (word_now.find(")") != -1){
                sel.is_count = true;
            } else {
                int dot_pos = word_now.find(".");
                if(dot_pos != -1){
                    sel.col.tablename = word_now.substr(0, pos);
                    sel.col.column_name = word_now.substr(pos+1);
                } else {
                    sel.col.column_name = word_now;
                }
            }
            select_op -> selectors.push_back(sel);
            word_now = get_input_word();
            if (word_now != ",") break;
        }
        while(1){
            word_now = get_input_word();
            select_op -> table_names.push_back(word_now);
            word_now = get_input_word();
            if (word_now != ",") break;
        }
        WhereClauses* where_info = new WhereClauses();
        if (word_now == "WHERE"){
            read_where_clauses(where_info);
        }
        select_op -> info = where_info;
        ret_val = select_op;
    } else if (word_now == "ALTER") {
        get_input_word();
        std::string table_name = get_input_word();
        std::string op = get_input_word();
        if (op == "ADD"){
            word_now = get_input_word();
            if(word_now == "INDEX"){
                get_input_word();
                std::string col_name = get_input_word();
                ret_val = new AddIndexOp(table_name, col_name);
            } else if (word_now == "CONSTRAINT") {
                std::string constraint_name = "";
                word_now = get_input_word();
                if (word_now != "PRIMARY" && word_now != "FOREIGN"){
                    constraint_name = word_now;
                    word_now = get_input_word();
                }
                get_input_word();
                get_input_word();
                if(word_now == "PRIMARY") {
                    AddPkOp* add_pk = new AddPkOp();
                    add_pk -> pk_field.this_table_name = table_name;
                    add_pk -> pk_field.pk_name = constraint_name;
                    add_pk -> pk_field.pk_column = get_input_word();
                    ret_val = add_pk;
                } else if (word_now == "FOREIGN") {
                    AddFkOp* add_fk = new AddFkOp();
                    add_fk -> fk_field.this_table_name = table_name;
                    add_fk -> fk_field.fk_name = constraint_name;
                    add_fk -> fk_field.this_table_column = get_input_word();
                    get_input_word();
                    get_input_word();
                    add_fk -> fk_field.foreign_table_name = get_input_word();
                    get_input_word();
                    add_fk -> fk_field.foreign_table_column = get_input_word();
                    ret_val = add_fk;
                } else {
                    ret_val = new ErrorOp(input, word_now);
                }
            } else if (word_now == "UNIQUE") {
                get_input_word();
                std::string col_name = get_input_word();
                ret_val = new AddUniqueOp(table_name, col_name);
            } else {
                ret_val = new ErrorOp(input, word_now);
            }
        } else if (op == "DROP") {
            word_now = get_input_word();
            if(word_now == "INDEX"){
                get_input_word();
                std::string column = get_input_word();
                ret_val = new DropIndexOp(table_name, column);
            } else if (word_now =="PRIMARY"){
                get_input_word();
                std::string primary_name = get_input_word();
                if(primary_name == "\0") primary_name = "";
                ret_val = new DropPkOp(table_name, primary_name);
            } else if (word_now == "FOREIGN") {
                get_input_word();
                std::string foreign_name = get_input_word();
                ret_val = new DropFkOp(table_name, foreign_name);
            } else {
                ret_val = new ErrorOp(input, word_now);
            }
        }
    } else if (word_now == "LOAD"){
        get_input_word();
        get_input_word();
        input_value file_name = read_input_value();
        get_input_word();
        get_input_word();
        std::string table_name = get_input_word(); 
        ret_val = new LoadOp(file_name.value, table_name);
    } else if (word_now == "DUMP"){
    
    } else if (word_now == "EXIT" || word_now == "QUIT"){
        ret_val = new QuitOp();
    } else { // not recognized
        ret_val = new ErrorOp(input, word_now);
    }
    return ret_val;
}

inline char SQLParser::read_forward(){
    if(pos==input.length()){
        return '\0';
    } else {
        return input[pos++];
    }
}

inline void SQLParser::read_backward(){
    pos = pos ? (pos-1) : pos;
}

std::string SQLParser::get_input_word(){
    std::string word = "";
	char c;
	while (1) {
		c = read_forward();
		if (c == '\0' || c == ',' || c == '(' || c == ')') {
			word.push_back(c); return word;
		}
		if (c != ' ' && c != '\0') break;
	}
	word.push_back(c);
	while (1) {
		c = read_forward();
        if (c == '=' || c =='<' || c == '>'){
            if(word == "<" || word == ">"){
                word.push_back(c);
            } else {
                break;
            }
        } else if (c != ' ' && c != '\0' && c != ',' && c != '(' && c != ')' && c != '='){
            word.push_back(c);
        } else {
            break;
        }
	}
	read_backward();
	return word;
}

input_value SQLParser::read_input_value(){
    std::string value = "";
    int ret_type = 0;
	char c;
	while (1) {
		c = read_forward();
		if (c == '\0') {return input_value(value,NO_TYPE);}
		if (c != ' ' && c != '\0' && c != ',' && c != '(') break;
	}
	if (c == ')') {
		value = ")";
        return input_value(value,NO_TYPE);
	}
    if (c == '\'') {
		ret_type = STRING_TYPE;
	} else if (!is_int_or_float(c) && c != '-'){
        read_backward();
        value = get_input_word();
        return input_value(value,NULL_TYPE);
    } else {
		value.push_back(c);
		ret_type = INT_TYPE;
	}
	while (1) {
		c = read_forward();
		if (ret_type == STRING_TYPE) {
			if (c == '\'') break; else {
				value.push_back(c);
				continue;
			}
		}
		if (ret_type != STRING_TYPE && (c == '.' || c == 'e')) ret_type = FLOAT_TYPE;
		if (c != ' ' && c != '\0' && c != ',' && c != '(' && c != ')' && c != '\'') value.push_back(c); else break;
	}
	if (c != '\'') read_backward();
	return input_value(value,ret_type);
}

void SQLParser::read_value_list(InsertInfo* insert_info){
    while(1){
        input_attribute attri;
        while (1) {
            input_value input_val = read_input_value();
            if (input_val.value == ")") break;
            if (input_val.ret_type) {
                attri.values.push_back(input_val);
            }
        }
        if(attri.values.size()==0) return;
        insert_info->attributes.push_back(attri);
        std::string temp = get_input_word();
        if(temp!=",") {return;}
    }
}

inline bool SQLParser::is_int_or_float(char c){
    return (c=='0') || (c=='1') || (c=='2') || (c=='3') || (c=='4') || (c=='5') || (c=='6') || (c=='7') ||
        (c=='8') ||(c=='9');
}

bool SQLParser::read_where_clauses(WhereClauses* wcs){
    while(1){
        WhereClause wc;
        std::string word_now = get_input_word();
        int dot_pos = word_now.find('.');
        if (dot_pos != -1){
            wc.l_col.tablename = word_now.substr(0, pos);
            wc.l_col.column_name = word_now.substr(pos+1);
        } else {
            wc.l_col.column_name = word_now;
            // wc.l_col.tablename = table_name;
        }
        std::string cond_op = get_input_word();
        if (cond_op == "IS"){ // is or is not NULL
            word_now = get_input_word();
            if (word_now == "NOT"){
                wc.is_not_Null = true;
                get_input_word();
            } else {
                wc.is_Null = true;
            }
        } else if (cond_op == "IN") { // in value_list
            wc.in_value_list = true;
            while (1) {
                input_value input_val = read_input_value();
                if (input_val.value == ")") break;
                if (input_val.ret_type) {
                    wc.value_list.values.push_back(input_val);
                }
            }
            if(wc.value_list.values.size()==0) return false;
        } else { // arithmetic
            wc.is_operand = true;
            if (cond_op == "="){
                wc.operand = EQUAL;
            } else if (cond_op == "<") {
                wc.operand = LESS;
            } else if (cond_op == "<=") {
                wc.operand = LESS_EQUAL;
            } else if (cond_op == ">") {
                wc.operand = GREATER;
            } else if (cond_op == ">=") {
                wc.operand = GREATER_EQUAL;
            } else if (cond_op == "<>") {
                wc.operand = NOT_EQUAL;
            } else {
                return false;
            }
            input_value in = read_input_value();

            if (in.value != "NULL" && in.ret_type == NULL_TYPE) {
                wc.use_r_col = true;
                dot_pos = in.value.find('.');
                if (dot_pos != -1){
                    wc.r_col.tablename = in.value.substr(0, pos);
                    wc.r_col.column_name = in.value.substr(pos+1);
                } else {
                    // wc.r_col.tablename = table_name;
                    wc.r_col.column_name = in.value;
                }
            } else {
                wc.r_val = in;
                wc.use_r_col = false;
            }
        }
        wcs->clauses.push_back(wc);
        std::string temp = get_input_word();
        if (temp != "AND") break;
    }
    return true;
}

bool SQLParser::read_create_table(TableInfo* table_info){
    std::string temp = get_input_word();
    if(temp != "("){
        return false;
    }
    while(1){
        input_field in_field;
        std::string word_now = get_input_word();
        if (word_now == "PRIMARY"){
            in_field.is_pk = true;
            in_field.pk_field.this_table_name = table_info->table_name;
            get_input_word();
            word_now = get_input_word();
            if (word_now != "("){
                in_field.pk_field.pk_name = word_now;
                word_now = get_input_word();
            }
            word_now = get_input_word();
            in_field.pk_field.pk_column = word_now;
            get_input_word();
            word_now = get_input_word();
        } else if (word_now == "FOREIGN") {
            in_field.is_fk = true;
            in_field.fk_field.this_table_name = table_info->table_name;
            get_input_word();
            word_now = get_input_word();
            if (word_now != "("){
                in_field.fk_field.fk_name = word_now;
                word_now = get_input_word();
            }
            word_now = get_input_word();
            in_field.fk_field.this_table_column = word_now;
            word_now = get_input_word();
            get_input_word();
            in_field.fk_field.foreign_table_name = get_input_word();
            get_input_word();
            in_field.fk_field.foreign_table_column = get_input_word();
            get_input_word();
            word_now = get_input_word();
        } else {
            in_field.is_normal = true;
            in_field.nor_field.column_name = word_now;
            std::string type_ = get_input_word();
            if (type_ == "INT"){
                in_field.nor_field.type = INT_TYPE;
            } else if (type_ == "FLOAT") {
                in_field.nor_field.type = FLOAT_TYPE;
            } else if (type_ == "VARCHAR"){
                in_field.nor_field.type = STRING_TYPE;
                if(get_input_word() != "("){
                    return false;
                }
                in_field.nor_field.string_len = atoi(get_input_word().c_str());
                get_input_word();
            } else {
                return false;
            }
            word_now = get_input_word();
            if(word_now == "NOT"){
                get_input_word();
                in_field.nor_field.not_null_required = true;
                word_now = get_input_word();
            }
            if(word_now == "DEFAULT"){
                in_field.nor_field.has_default = true;
                in_field.nor_field.default_value = read_input_value();
                word_now = get_input_word();
            }
        }
        table_info->fields.push_back(in_field);
        if (word_now != ",") break;
    }
    return true;
}

bool SQLParser::read_set_clauses(UpdateOp* update_op){
    std::string temp;
    while(1) {
        SetClause sc;
        sc.column_name = get_input_word();
        if (get_input_word() != "=") return false;
        sc.update_value = read_input_value();
        update_op->set_clauses.push_back(sc);
        temp = get_input_word();
        if(temp != ",") break;
    }
    return true;
}
