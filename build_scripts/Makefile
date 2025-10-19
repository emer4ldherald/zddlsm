CUR_DIR = $(CURDIR)

all: bddc bdd+
	@echo "All external libraries built successfully!"

bddc:
	$(MAKE) -f $(CUR_DIR)/bddc.mk 64

bdd+:
	$(MAKE) -f $(CUR_DIR)/bdd+.mk 64

clean:
	@echo "Cleaning all external builds..."
	$(MAKE) -f $(CUR_DIR)/bdd+.mk clean
	$(MAKE) -f $(CUR_DIR)/bddc.mk clean