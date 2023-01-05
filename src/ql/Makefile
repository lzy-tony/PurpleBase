CC = g++
STD11 = -std=c++11
DEPS = info.h sqlop.h sqlparser.h
OBJ = sqlparser.o
OPT = -O3

main: $(OBJ)

sqlparser.o: sqlparser.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)
