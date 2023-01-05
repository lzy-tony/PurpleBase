#pragma once
#include "sqlop.h"
#include "../sm/sm.h"

class ql_manager{
    SM_Manager* sm;
public:
    ql_manager(SM_Manager* _sm):sm(_sm){}
    inline int get_table_index(std::string&);
    inline void error_when_insert(input_attribute&, const char*);
    inline bool match_type(int, int);
    void Insert(InsertOp*);
    void Select();
};