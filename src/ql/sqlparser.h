#pragma once
#include <string>
#include "sqlop.h"

class SQLParser{
    std::string input;
    int pos;
public:
    OpBase* parse(std::string&);
    std::string get_input_word();
    inline char read_forward();
    inline void read_backward();
    inline bool is_comment(std::string& i) { return (i.length()>=2) && (i.substr(0,2)=="--");}
    void read_value_list(InsertInfo*);
    input_value read_input_value();
    bool read_where_clauses(WhereClauses*);
    inline bool is_int_or_float(char c);
    bool read_create_table(TableInfo*);
    bool read_set_clauses(UpdateOp* update_op);
};