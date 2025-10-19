CC	= g++
DIR	= ../SAPPOROBDD
INCL    = $(DIR)/include
OPT	= -O3 -Wall -Wextra -Wshadow -I$(INCL) -fPIC
OPT64   = $(OPT) -DB_64
LDFLAGS = -shared
SRC = ../SAPPOROBDD/src/BDD+

LIB32	= $(DIR)/lib/libBDD32.so
LIB64	= $(DIR)/lib/libBDD64.so
OBJC32	= $(DIR)/src/BDDc/*_32.o
OBJC64	= $(DIR)/src/BDDc/*_64.o

all:    $(LIB64)

32:     $(LIB32)

64:     $(LIB64)

$(DIR)/lib:
	mkdir -p $(DIR)/lib

$(LIB64): $(SRC)/BDD_64.o $(SRC)/ZBDD_64.o | $(DIR)/lib
	rm -f $(LIB64)
	$(CC) $(LDFLAGS) -o $(LIB64) $(SRC)/*_64.o $(OBJC64)

$(LIB32): $(SRC)/BDD_32.o $(SRC)/ZBDD_32.o | $(DIR)/lib
	rm -f $(LIB32)
	$(CC) $(LDFLAGS) -o $(LIB32) $(SRC)/*_32.o $(OBJC32)


clean: 
	rm -f $(SRC)/*.o $(SRC)/*~

$(SRC)/BDD_32.o: $(SRC)/BDD.cc $(INCL)/BDD.h
	$(CC) $(OPT) -c $(SRC)/BDD.cc -o $(SRC)/BDD_32.o

$(SRC)/BDD_64.o: $(SRC)/BDD.cc $(INCL)/BDD.h
	$(CC) $(OPT64) -c $(SRC)/BDD.cc -o $(SRC)/BDD_64.o

$(SRC)/ZBDD_32.o: $(SRC)/ZBDD.cc $(INCL)/ZBDD.h $(INCL)/BDD.h
	$(CC) $(OPT) -c $(SRC)/ZBDD.cc -o $(SRC)/ZBDD_32.o

$(SRC)/ZBDD_64.o: $(SRC)/ZBDD.cc $(INCL)/ZBDD.h $(INCL)/BDD.h
	$(CC) $(OPT64) -c $(SRC)/ZBDD.cc -o $(SRC)/ZBDD_64.o
