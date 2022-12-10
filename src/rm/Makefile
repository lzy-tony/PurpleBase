CC = g++
STD11 = -std=c++11
DEPS = rm.h
OBJ = rm_manager.o rm_filehandle.o
OPT = -O3

main: $(OBJ)

%.o: %.cpp %(DEPS)
	$(CC) $< -o $@ $(STD11) $(OPT)

clean:
	rm *.o