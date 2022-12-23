CC = g++
STD11 = -std=c++11
DEPS = ix.h
OBJ = ix_manager.o ix_indexhandle.o
OPT = -O3

main: $(OBJ)

ix_test: ix_test.cpp ix_manager.cpp ix_indexhandle.cpp ../pf/pf.cpp ix.h
	g++ ix_test.cpp ix_manager.cpp ix_indexhandle.cpp ../pf/pf.cpp -o ix_test

ix_manager.o: ix_manager.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)

ix_indexhandle.o: ix_indexhandle.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)

clean:
	rm *.o

clean_test:
	rm ix_test