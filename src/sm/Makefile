CC = g++
STD11 = -std=c++11
DEPS = sm.h
OBJ = sm_manager.o
OPT = -O3

main: $(OBJ)

sm_manager.o: sm_manager.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)

clean:
	rm *.o