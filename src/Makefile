CC = g++
STD11 = -std=c++11
OPT = -O3

main: main.cpp
	cd pf && $(CC) -c pf.cpp $(STD11) $(OPT)
	cd rm && make
	cd ix && make
	cd sm && make
	cd ql && make
	$(CC) -c main.cpp $(STD11) $(OPT)
	$(CC) main.o pf/pf.o rm/rm_manager.o rm/rm_filehandle.o rm/rm_filescan.o ix/ix_manager.o ix/ix_indexhandle.o sm/sm_manager.o ql/sqlparser.o -o main $(STD11) $(OPT)

clean:
	rm main