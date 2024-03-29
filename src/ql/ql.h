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
    inline bool comp_op(float, float, int);
    inline bool comp_op(unsigned int, unsigned int, int);
    inline bool comp_op(std::string&,std::string&, int);
    inline int get_primary_key(int tid);


    void Insert(InsertOp*);
    void Update(UpdateOp*);
    void Delete(DeleteOp*);
    void Select(SelectOp*);
    
    void Load(LoadOp*);

    int match_record(BufType, int, std::vector<WhereClause>&, int);
    bool validate_column(Column&, std::vector<std::string>&);
    bool validate_where_clause(WhereClauses*, std::vector<std::string>&);
    void display_result(std::vector<int>&, std::vector<BufType>&, std::string&);
    std::string data_to_string(BufType, int, int);
    void Select_one_table(std::vector<WhereClause>&, std::vector<BufType>&, std::string&);
    void Select_two_table(SelectOp*, std::vector<WhereClause>&, std::vector<BufType>&, std::vector<BufType>&, std::vector<int>&, std::vector<int>&);
};