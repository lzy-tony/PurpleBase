CC = g++
STD11 = -std=c++11
DEPS = rm.h
OBJ = rm_manager.o rm_filehandle.o rm_filescan.o
OPT = -O3

main: $(OBJ)

rm_test: rm_test.cpp rm_manager.cpp rm_filehandle.cpp rm_filescan.cpp ../pf/pf.cpp rm.h
	g++ rm_test.cpp rm_manager.cpp rm_filehandle.cpp rm_filescan.cpp ../pf/pf.cpp -o rm_test

rm_manager.o: rm_manager.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)

rm_filehandle.o: rm_filehandle.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)

rm_filescan.o: rm_filescan.cpp $(DEPS)
	$(CC) -c $< $(STD11) $(OPT)

rm_test.o: rm_test.cpp $(DEPS)

clean:
	rm *.o

clean_test:
	rm rm_test