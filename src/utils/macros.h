#ifndef MACROS_H
#define MACROS_H

#define INF 0x3f3f3f3f

enum CompOp {
    EQ_OP,
    LT_OP,
    GT_OP,
    LE_OP,
    GE_OP,
    NE_OP,
    NO_OP,
};

enum AttrType {
    INT_TYPE, FLOAT_TYPE, STRING_TYPE,
};

#endif