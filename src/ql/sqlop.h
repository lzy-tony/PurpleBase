#pragma once
#include <string>
#include <cstdio>
#include "info.h"

enum{
    ERROR_OP,
    SHOW_DATABASE_OP,
    SHOW_TABLE_OP,
    USE_DATABASE_OP,
    CREATE_DATABASE_OP,
    CREATE_TABLE_OP,
    DROP_DATABASE_OP,
    DROP_TABLE_OP,
    DESC_TABLE_OP,
    INSERT_OP,
    DELETE_OP,
    UPDATE_OP,
    SELECT_OP,
    ADD_INDEX_OP,
    DROP_INDEX_OP,
    ADD_PK_OP,
    DROP_PK_OP,
    ADD_FK_OP,
    DROP_FK_OP,
    ADD_UNIQUE_OP,
    QUIT_OP,
};

class OpBase{
public:
    int op_type;
    virtual void give_info() = 0;
    Info* info = nullptr;
};

class ErrorOp: public OpBase{
public:
    std::string error_in;
    std::string error_word;
    ErrorOp(std::string& input, std::string& _error_word): error_in(input),error_word(_error_word) {op_type=ERROR_OP;}
    void give_info () { printf("error occur when parsing input %s, the word occured error is %s \n",error_in.c_str(), error_word.c_str()); }
};

class ShowDatabaseOp: public OpBase{
public:
    ShowDatabaseOp() { op_type=SHOW_DATABASE_OP; }
    void give_info() { printf("show all databases\n");}
};

class ShowTablesOp: public OpBase{
public:
    ShowTablesOp() {op_type=SHOW_TABLE_OP;}
    void give_info() {printf("show all tables in current database If a database has been used\n");}
};

class UseDataBaseOp: public OpBase{
public:
    std::string db_name;
    UseDataBaseOp(std::string& _db_name): db_name(_db_name) {op_type = USE_DATABASE_OP;}
    void give_info() {printf("start using database named %s \n", db_name.c_str());}
};

class CreateDatabaseOp: public OpBase{
public:
    std::string db_name;
    CreateDatabaseOp(std::string& _db_name): db_name(_db_name) {op_type = CREATE_DATABASE_OP;}
    void give_info() {printf("create a database named %s\n", db_name.c_str());}
};

class CreateTableOp: public OpBase{
public:
    std::string table_name;
    void give_info(){info->give_info();}
    CreateTableOp(){op_type = CREATE_TABLE_OP;}
    ~CreateTableOp(){
        if(info) delete info;
    }
};

class DropDatabaseOp: public OpBase{
public:
    std::string db_name;
    DropDatabaseOp(std::string& _db_name): db_name(_db_name) {op_type = DROP_DATABASE_OP;}
    void give_info() {printf("drop a database named %s\n",db_name.c_str());}
};

class DropTableOp: public OpBase{
public:
    std::string table_name;
    DropTableOp(std::string& _table_name): table_name(_table_name) {op_type = DROP_TABLE_OP;}
    void give_info() {printf("drop a table named %s\n",table_name.c_str());}
};

class DESCOp: public OpBase{
public:
    std::string table_name;
    DESCOp(std::string& _table_name): table_name(_table_name) {op_type = DESC_TABLE_OP;}
    void give_info() {printf("describe a table named %s\n",table_name.c_str());}
};

class InsertOp: public OpBase{
public:
    std::string table_name;
    InsertOp(std::string& _table_name): table_name(_table_name) {op_type = INSERT_OP;}
    void give_info() {
        printf("inserting into table %s\n", table_name.c_str());
        info->give_info();
    }
    ~InsertOp(){if(info) delete info;}
};

class DeleteOp: public OpBase{
public:
    std::string table_name;
    DeleteOp(std::string& _table_name): table_name(_table_name) {op_type = DELETE_OP;}
    void give_info() {
        printf("deleting from table %s , conditions are: \n", table_name.c_str());
        info -> give_info();
    }
    ~DeleteOp(){if(info) delete info;}
};

class UpdateOp: public OpBase{
public:
    std::string table_name;
    std::vector<SetClause> set_clauses;
    UpdateOp(std::string& _table_name): table_name(_table_name) {op_type = UPDATE_OP;}
    void give_info(){
        printf("updating these columns:\n");
    }
    ~UpdateOp(){if(info) delete info;}
};

class SelectOp: public OpBase{
public:
    std::vector<Selector> selectors;
    std::vector<std::string> table_names;
    SelectOp(){op_type = SELECT_OP;}
    void give_info(){
        printf("selecting from these tables:");
        for (auto name:table_names){
            printf(" %s,", name.c_str());
        }
        printf("\n");
        info->give_info();
    }
    ~SelectOp(){if(info) delete info;}
};

class AddIndexOp: public OpBase{
public:
    std::string table_name;
    std::string index_column;
    AddIndexOp(std::string& _table_name, std::string& _column_name):table_name(_table_name),index_column(_column_name){op_type = ADD_INDEX_OP;}
    void give_info(){ printf("Add column %s in table %s as index\n",index_column.c_str(), table_name.c_str());}
};

class DropIndexOp: public OpBase{
public:
    std::string table_name;
    std::string index_column;
    DropIndexOp(std::string& _table_name, std::string& _column_name):table_name(_table_name),index_column(_column_name){op_type = DROP_INDEX_OP;}
    void give_info() {printf("drop index column %s in table %s\n",index_column.c_str(), table_name.c_str());}
};

class DropPkOp: public OpBase{
public:
    std::string table_name;
    std::string pk_name;
    DropPkOp(std::string& _table_name, std::string& _pk_name):table_name(_table_name), pk_name(_pk_name){op_type = DROP_PK_OP;}
    void give_info(){printf("Drop pk in table %s\n",table_name.c_str());}
};

class DropFkOp: public OpBase{
public:
    std::string table_name;
    std::string fk_name;
    DropFkOp(std::string& _table_name, std::string& _fk_name):table_name(_table_name), fk_name(_fk_name){op_type = DROP_FK_OP;}
    void give_info() {printf("Drop fk %s in table %s\n", fk_name.c_str(), table_name.c_str());}
};

class AddPkOp: public OpBase{
public:
    void give_info(){;}
    AddPkOp(){op_type = ADD_PK_OP;}
    primary_key_field pk_field;
};

class AddFkOp: public OpBase{
public:
    void give_info(){;}
    AddFkOp(){op_type = ADD_FK_OP;}
    foreign_key_field fk_field;
};

class AddUniqueOp: public OpBase{
public:
    std::string column_name;
    std::string table_name;
    AddUniqueOp(std::string _table_name, std::string& _in):table_name(_table_name), column_name(_in) {op_type = ADD_UNIQUE_OP;}
    void give_info(){;}
};

class QuitOp: public OpBase{
public:
    QuitOp(){op_type = QUIT_OP;}
    void give_info(){}
};
