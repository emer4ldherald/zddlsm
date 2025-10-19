CC    = gcc
DIR   = ../SAPPOROBDD
INCL  = $(DIR)/include
LIB   = $(DIR)/lib
OPT   = -O3 -Wall -Wextra -Wshadow -I$(INCL) -fPIC
OPT64 = $(OPT) -DB_64
LDFLAGS = -shared
SRC = ../SAPPOROBDD/src/BDDc

all: $(LIB)/libbddc64.so

32: $(LIB)/libbddc32.so

64: $(LIB)/libbddc64.so

$(LIB):
	mkdir -p $(LIB)

$(SRC)/bddc_32.o: $(SRC)/bddc.c $(INCL)/bddc.h
	$(CC) $(OPT) -c $(SRC)/bddc.c -o $(SRC)/bddc_32.o

$(SRC)/bddc_64.o: $(SRC)/bddc.c $(INCL)/bddc.h
	$(CC) $(OPT64) -c $(SRC)/bddc.c -o $(SRC)/bddc_64.o

$(LIB)/libbddc32.so: $(SRC)/bddc_32.o | $(LIB)
	$(CC) $(LDFLAGS) -o $@ $<

$(LIB)/libbddc64.so: $(SRC)/bddc_64.o | $(LIB)
	$(CC) $(LDFLAGS) -o $@ $<

clean:
	rm -f $(SRC)/*.o $(SRC)/*.so $(SRC)/*~
	rm -rf $(LIB)