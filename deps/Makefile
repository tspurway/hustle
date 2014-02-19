# Top level Makefile, liblmdb is the only thing needs tobe compiled

RM = rm -rf
PYTHON = python
LMDB = liblmdb

default: all

.DEFAULT:
	cd $(LMDB) && $(MAKE) $@
	$(PYTHON) setup.py build

install:
	cd $(LMDB) && $(MAKE) $@
	$(PYTHON) setup.py install

.PHONY: clean

clean:
	cd $(LMDB) && $(MAKE) $@
	$(RM) build
