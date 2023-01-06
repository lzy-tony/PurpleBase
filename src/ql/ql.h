#pragma once
#include "sqlop.h"
#include "../sm/sm.h"

class ql_manager{
    SM_Manager* sm;
    FileManager *fm;
    BufPageManager *bpm;
    RM_Manager *rm;
    IX_Manager *ix;
public:
    ql_manager(SM_Manager* _sm,FileManager *_fm, BufPageManager *_bpm, RM_Manager *_rm, IX_Manager *_ix):
        sm(_sm), fm(_fm), bpm(_bpm), rm(_rm), ix(_ix){}
    inline int get_table_index(std::string&);
    inline int get_column_index(int, std::string&);
    inline void error_when_insert(input_attribute&, const char*);
    inline bool match_type(int, int);
    inline bool table_has_column(std::string&, std::string&);
    inline bool is_index_column(std::string&,std::string&);
    inline bool need_next(int);
    inline bool comp_op(double, double, int);
    inline bool comp_op(unsigned int, unsigned int, int);

    void Insert(InsertOp*);
    void Update(UpdateOp*); //TODO
    void Delete(DeleteOp*); //TODO
    void Select(SelectOp*);

    int match_record(BufType, int, std::vector<WhereClause>&, int);
    void Select_one_table(SelectOp*);
    void Select_two_table(SelectOp*);
};