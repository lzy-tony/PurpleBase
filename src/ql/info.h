#pragma once
#include <vector>
#include <cstdio>
#include <string>

enum{
    NO_TYPE,
    INT_TYPE,
    FLOAT_TYPE,
    STRING_TYPE,
    NULL_TYPE,
};

enum{
    EQUAL,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
    NOT_EQUAL,
};

struct input_value{
    std::string value;
    int ret_type;
    input_value(std::string& _v, int _ret):value(_v), ret_type(_ret){}
    input_value(){}
    void print(){printf("value %s type %d ,",value.c_str(),ret_type);}
};

struct input_attribute{
    std::vector<input_value> values;
    input_attribute(){values.clear();}
    void print(){
        printf("inserting values :");
        for (auto val: values){
            val.print();
        }
        printf("\n");
    }
};

struct primary_key_field{
    std::string this_table_name;
    std::string pk_name;
    std::string pk_column;
};

struct foreign_key_field{
    std::string this_table_name;
    std::string this_table_column;
    std::string foreign_table_name;
    std::string foreign_table_column;
    std::string fk_name;
};

struct normal_key_field{
    input_value default_value;
    bool has_default;
    bool not_null_required;
    int type;
    int string_len;
    std::string column_name;
};

struct input_field{
    bool is_normal = false;
    bool is_pk = false;
    bool is_fk = false;
    primary_key_field pk_field;
    foreign_key_field fk_field;
    normal_key_field nor_field;
    void print(){
        if(is_normal){
            printf("normal field named %s, type is %d ", nor_field.column_name.c_str(), nor_field.type);
            if(nor_field.type == STRING_TYPE) printf("with length (%d) ",nor_field.string_len);
            if(nor_field.not_null_required){
                printf("not null required ,");
            }
            if(nor_field.has_default){
                printf("default value required, its value is %s with type %d",
                nor_field.default_value.value.c_str(), nor_field.default_value.ret_type);
            }
        }
        if(is_pk){
            printf("primary key constraint on column %s,", pk_field.pk_column.c_str());
            if(pk_field.pk_name != "") printf("constraint named %s ", pk_field.pk_name.c_str());
        }
        if(is_fk){
            printf("foreign key on column %s, other table named %s, column is %s",
            fk_field.this_table_column.c_str(),fk_field.foreign_table_name.c_str(),fk_field.foreign_table_column.c_str());
            if(fk_field.fk_name != "") printf("constraint named %s ", fk_field.fk_name.c_str());
        }
    }
};

enum INFOTYPE{
    INSERT_INFO,
    WHERE_CLAUSE_INFO,
    TABLE_INFO,
};

class Info{
public:
    int info_type;
    virtual void give_info() = 0;
};


class TableInfo : public Info{
public:
    void give_info(){
        printf("creating table named %s , its field are shown below:\n", table_name.c_str());
        for (auto field : fields){
            field.print();
        }
    }
    std::string table_name;
    std::vector<input_field> fields;
    TableInfo(std::string _table_name): table_name(_table_name){}
};

class InsertInfo: public Info{
public:
    void give_info(){
        for (auto attri: attributes){
            attri.print();
        }
    }
    std::vector<input_attribute> attributes;
};

struct Column{
    std::string tablename;
    std::string column_name;
    Column(std::string& _t,std::string& _c):tablename(_t),column_name(_c){}
    Column(){}
};

struct Selector{
    Column col;
    bool is_count = false;
};

struct WhereClause{
    Column l_col;

    bool is_operand;
    bool use_r_col;
    int operand;
    Column r_col;
    input_value r_val;

    bool in_value_list;
    input_attribute value_list;

    bool is_Null;
    bool is_not_Null;
    WhereClause(): is_operand(false), use_r_col(false), in_value_list(false), is_Null(false), is_not_Null(false){}
    void give_info(){
        printf("left column: %s from table %s ,",l_col.column_name.c_str(),l_col.tablename.c_str());
        if(is_operand){
            printf("operand type %d ,",operand);
            if(use_r_col){
                printf("right column: %s from table %s ",r_col.column_name.c_str(),r_col.tablename.c_str());
            } else {
                printf("right");
                r_val.print();
            }
        }
        if(in_value_list){
            printf("in value list ");
            value_list.print();
        }
        if(is_Null){
            printf("is NULL");
        }
        if (is_not_Null){
            printf("is not Null");
        }
        printf("\n");
    }
};

class WhereClauses: public Info{
public:
    std::vector<WhereClause> clauses;
    void give_info() {
        for(auto clause:clauses){
            clause.give_info();
        }
    }
};

struct SetClause{
    std::string column_name;
    input_value update_value;
};
