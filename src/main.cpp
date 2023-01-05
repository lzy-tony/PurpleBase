#include <iostream>
#include <cstdio>
#include <string>
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

    

    while (1) {
        if (use) {
            std::cout << db_name << "> ";
        } else {
            std::cout << "> ";
        }

        if (!getline(cin, command)) {
            break;
        }

        std::cout << command << std::endl;


    }

    return 0;
}