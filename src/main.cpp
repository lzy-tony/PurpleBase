#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <set>
#include <fstream>
#include <unistd.h>

#include "pf/pf.h"
#include "rm/rm.h"
#include "ix/ix.h"
#include "sm/sm.h"
#include "ql/sqlparser.h"
#include "ql/ql.h"

int main() {
    MyBitMap::initConst();
    chdir("../runs");
    bool use = false;
    std::string db_name;
    std::string command;

    FileManager *fm = new FileManager();
    BufPageManager *bpm = new BufPageManager(fm);
    RM_Manager *rm = new RM_Manager(fm, bpm);
    IX_Manager *ix = new IX_Manager(fm, bpm);
    SM_Manager *sm = new SM_Manager(fm, bpm, rm, ix);
    SQLParser *parser = new SQLParser();
    ql_manager *ql = new ql_manager(sm,fm,bpm,rm,ix);
    

    // load database info
    std::set <std::string> databases;
    databases.clear();
    ifstream db_meta("info.db");
    if (!db_meta.fail()) {
        int db_num;
        db_meta >> db_num;
        for (int i = 0; i < db_num; i++) {
            std::string db_name;
            db_meta >> db_name;
            databases.insert(db_name);
        }
    }
    db_meta.close();

    while (1) {
        if (use) {
            std::cout << db_name << "> ";
            fflush(stdout);
        } else {
            std::cout << "> ";
            fflush(stdout);
        }
        if (!getline(cin, command)) {
            break;
        }
        auto op = parser -> parse(command);
        if (op -> op_type == CREATE_DATABASE_OP) {
            CreateDatabaseOp* create_db_op = dynamic_cast<CreateDatabaseOp*> (op);
            if (databases.find(create_db_op -> db_name) == databases.end()) {
                databases.insert(create_db_op -> db_name);
                if (use) {
                    chdir("..");
                }
                std::string mkdir = "mkdir " + create_db_op -> db_name;
                system(mkdir.c_str());
                if (use) {
                    chdir(db_name.c_str());
                }
            } else {
                std::cout << "Error: Database already exist!" << std::endl;
            }
        } else if (op -> op_type == DROP_DATABASE_OP) {
            DropDatabaseOp *drop_db_op = dynamic_cast<DropDatabaseOp*> (op);
            if (databases.find(drop_db_op -> db_name) == databases.end()) {
                std::cout << "Error: Database does not exist!" << std::endl;
            } else {
                databases.erase(drop_db_op -> db_name);
                if (use) {
                    chdir("..");
                }
                std::string drop = "rm -rf " + drop_db_op -> db_name;
                system(drop.c_str());
                if (use) {
                    if (db_name == drop_db_op -> db_name) {
                        use = false;
                        db_name = "";
                    } else {
                        chdir(db_name.c_str());
                    }
                }
            }
        } else if (op -> op_type == SHOW_DATABASE_OP) {
            std::cout << "Databases" << std::endl;
            for (auto iter = databases.begin(); iter != databases.end(); iter++) {
                std::cout << *iter << std::endl;
            }
            std::cout << databases.size() << " row(s) in set" << std::endl;
        } else if (op -> op_type == USE_DATABASE_OP) {
            UseDataBaseOp *use_db_op = dynamic_cast<UseDataBaseOp*> (op);
            if (databases.find(use_db_op -> db_name) == databases.end()) {
                std::cout << "Error: No such database!" << std::endl;
            } else {
                if (use) {
                    sm -> CloseDB();
                } else {
                    use = true;
                }
                sm -> OpenDB(use_db_op -> db_name);
            }
            db_name = use_db_op -> db_name;
        } else if (op -> op_type == QUIT_OP) {
            std::cout << "Bye." << std::endl;
            break;
        } else if (op -> op_type == ERROR_OP) {
            ErrorOp *error_op = dynamic_cast<ErrorOp*> (op);
            // error_op -> give_info();
            std::cout << "Invalid syntax!" << std::endl;
        } else {
            if (!use) {
                std::cout << "Error: No database selected." << std::endl;
                continue;
            } 
            if (op -> op_type == CREATE_TABLE_OP) {
                CreateTableOp *create_table_op = dynamic_cast<CreateTableOp*> (op);

                op->give_info();

                TableInfo *table_info = dynamic_cast<TableInfo*> (op -> info);
                TableMeta *new_table = new TableMeta;
                new_table -> tableName = table_info -> table_name;
                new_table -> attrNum = 0;
                bool error = false;
                for (auto iter = table_info -> fields.begin(); iter != table_info -> fields.end(); iter++) {
                    if (iter -> is_normal) {
                        new_table -> attrNum++;
                        for (int i = 0; i < new_table -> attrs.size(); i++) {
                            if (new_table -> attrs[i].attrName == iter -> nor_field.column_name) {
                                error = true;
                                std::cout << "Error: Duplicate column name '" << iter -> nor_field.column_name << "'!" << std::endl;
                                break;
                            }
                        }
                        if (error) {
                            break;
                        }
                        AttrMeta attr;
                        attr.attrName = iter -> nor_field.column_name;
                        attr.isNotNULL = iter -> nor_field.not_null_required;
                        attr.isForeign = attr.isIndex = attr.isPrimary = false;
                        if (iter -> nor_field.has_default) {
                            attr.defaultValue = iter -> nor_field.default_value.value;
                        } else {
                            attr.defaultValue = "NULL";
                        }

                        if (iter -> nor_field.type == INT_TYPE) {
                            attr.attrType = INT_ATTRTYPE;
                        } else if (iter -> nor_field.type == FLOAT_TYPE) {
                            attr.attrType = FLOAT_ATTRTYPE;
                        } else if (iter -> nor_field.type == STRING_TYPE) {
                            attr.attrType = STRING_ATTRTYPE;
                            attr.attrLength = iter -> nor_field.string_len;
                        }
                        new_table -> attrs.push_back(attr);
                    } else if (iter -> is_pk) {
                        for (int i = 0; i < new_table -> attrs.size(); i++) {
                            if (new_table -> attrs[i].attrName == iter -> pk_field.pk_column) {
                                new_table -> attrs[i].isPrimary = true;
                                break;
                            }
                        }
                    } else if (iter -> is_fk) {
                        for (int i = 0; i < new_table -> attrs.size(); i++) {
                            if (new_table -> attrs[i].attrName == iter -> fk_field.this_table_column) {
                                if (new_table -> attrs[i].isForeign) {
                                    error = true;
                                    std::cout << "Error: Cannot add multiple foreign key constraint for a single column!" << std::endl;
                                    break;
                                }
                                new_table -> attrs[i].isForeign = true;
                                new_table -> attrs[i].referenceTable = iter -> fk_field.foreign_table_name;
                                new_table -> attrs[i].foreignKeyName = iter -> fk_field.foreign_table_column;
                                break;
                            }
                        }
                        if (error) {
                            break;
                        }
                    }
                }
                if (!error) {
                    sm -> CreateTable(new_table);
                }
            } else if (op -> op_type == DROP_TABLE_OP) {
                DropTableOp *drop_table_op = dynamic_cast<DropTableOp*> (op);
                sm -> DropTable(drop_table_op -> table_name); 
            } else if (op -> op_type == SHOW_TABLE_OP) {
                sm -> ShowTables();
            } else if (op -> op_type == DESC_TABLE_OP) {
                DESCOp *desc_table_op = dynamic_cast<DESCOp*> (op);
                sm -> DescribeTable(desc_table_op -> table_name);
            } else if (op -> op_type == INSERT_OP) {
                InsertOp* insert_op = dynamic_cast<InsertOp*>(op);
                ql -> Insert(insert_op);
            } else if (op -> op_type == DELETE_OP) {
                op->give_info();
                DeleteOp* delete_op = dynamic_cast<DeleteOp*>(op);
                ql -> Delete(delete_op);
            } else if (op -> op_type == UPDATE_OP) {
                // TODO: wait for ql
            } else if (op -> op_type == SELECT_OP) {
                SelectOp* select_op = dynamic_cast<SelectOp*>(op);
                ql -> Select(select_op);
            } else if (op -> op_type == ADD_INDEX_OP) {
                AddIndexOp *add_index_op = dynamic_cast<AddIndexOp*> (op);
                sm -> CreateIndex(add_index_op -> table_name, add_index_op -> index_column);
            } else if (op -> op_type == DROP_INDEX_OP) {
                DropIndexOp *drop_index_op = dynamic_cast<DropIndexOp*> (op);
                sm -> DropIndex(drop_index_op -> table_name, drop_index_op -> index_column);
            } else if (op -> op_type == ADD_PK_OP) {
                AddPkOp *add_pk_op = dynamic_cast<AddPkOp*> (op);
                std::cout << "add pk " << add_pk_op -> pk_field.this_table_name << " col " << add_pk_op -> pk_field.pk_column << std::endl;
                sm -> AddPrimaryKey(add_pk_op -> pk_field.this_table_name, add_pk_op -> pk_field.pk_column);
            } else if (op -> op_type == DROP_PK_OP) {
                DropPkOp *drop_pk_op = dynamic_cast<DropPkOp*> (op);
                sm -> DropPrimaryKey(drop_pk_op -> table_name);
            } else if (op -> op_type == ADD_FK_OP) {
                AddFkOp *add_fk_op = dynamic_cast<AddFkOp*> (op);
                sm -> AddForeignKey(add_fk_op -> fk_field.this_table_name, add_fk_op -> fk_field.this_table_column,
                                    add_fk_op -> fk_field.foreign_table_name, add_fk_op -> fk_field.foreign_table_column);
            } else if (op -> op_type == DROP_FK_OP) {
                DropFkOp *drop_fk_op = dynamic_cast<DropFkOp*> (op);
                sm -> DropForeignKey(drop_fk_op -> table_name, drop_fk_op -> fk_name);
            }
        }
    }

    if (use) {
        sm -> CloseDB();
    }

    ofstream db_out("info.db");
    db_out << databases.size() << "\n";
    for (auto iter = databases.begin(); iter != databases.end(); iter++) {
        db_out << *iter << "\n";
    }
    db_out.close();

    return 0;
}