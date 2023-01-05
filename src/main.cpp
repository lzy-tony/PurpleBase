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

int main() {
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
        } else {
            std::cout << "> ";
        }

        if (!getline(cin, command)) {
            break;
        }

        auto op = parser -> parse(command);
        if (op -> op_type == CREATE_DATABASE_OP) {
            CreateDatabaseOp* create_db_op = dynamic_cast<CreateDatabaseOp*> (op);
            if (databases.find(create_db_op -> db_name) == databases.end()) {
                databases.insert(create_db_op -> db_name);
                std::string mkdir = "mkdir " + create_db_op -> db_name;
                system(mkdir.c_str());
            } else {
                std::cout << "Error: Database already exists!" << std::endl;
            }
        } else if (op -> op_type == DROP_DATABASE_OP) {
            DropDatabaseOp *drop_db_op = dynamic_cast<DropDatabaseOp*> (op);
            
        } else if (op -> op_type == SHOW_DATABASE_OP) {
            std::cout << "Databases" << std::endl;
            for (auto iter = databases.begin(); iter != databases.end(); iter++) {
                std::cout << *iter << std::endl;
            }
            std::cout << databases.size() << " row(s) in set" << std::endl;
        } else if (op -> op_type == USE_DATABASE_OP) {

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