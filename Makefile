PG_CONFIG = pg_config

MODULE_big = recyclebin
OBJS = recyclebin.o

EXTENSION = recyclebin
DATA = recyclebin--0.5.sql

REGRESS = test
REGRESS_OPTS = --inputdir=test

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)