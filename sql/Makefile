# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-scheduler-dev
DOCDIR = autodoc

SQL_SCHEMA  = 30-swh-schema.sql
SQL_FUNC    = 40-swh-func.sql
SQL_DATA    = 50-swh-data.sql
SQL_INDEXES = 60-swh-indexes.sql
SQLS = $(SQL_SCHEMA) $(SQL_FUNC) $(SQL_DATA) $(SQL_INDEXES)
SQL_FILES = $(abspath $(addprefix $(CURDIR)/../swh/scheduler/sql/,$(SQLS)))

PSQL_BIN = psql
PSQL_FLAGS = --echo-all -X -v ON_ERROR_STOP=1
PSQL = $(PSQL_BIN) $(PSQL_FLAGS)

PIFPAF=$(findstring postgresql://,$(PIFPAF_URLS))

all:

createdb: createdb-stamp
createdb-stamp: $(SQL_FILES)
ifeq ($(PIFPAF),)
	-dropdb $(DBNAME)
endif
	createdb $(DBNAME)
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

filldb: filldb-stamp
filldb-stamp: createdb-stamp
	cat $(SQL_FILES) | $(PSQL) $(DBNAME)
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

dropdb:
	-dropdb $(DBNAME)

dumpdb: swh-scheduler.dump
swh-scheduler.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

$(DOCDIR):
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)

doc: autodoc-stamp $(DOCDIR)/swh-scheduler.pdf
autodoc-stamp: filldb-stamp $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh-scheduler
	cp -a $(DOCDIR)/swh-scheduler.dot $(DOCDIR)/swh-scheduler.dot.orig
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

$(DOCDIR)/swh-scheduler.pdf: $(DOCDIR)/swh-scheduler.dot autodoc-stamp
	dot -T pdf $< > $@
$(DOCDIR)/swh-scheduler.svg: $(DOCDIR)/swh-scheduler.dot autodoc-stamp
	dot -T svg $< > $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh-scheduler.dump

.PHONY: all initdb createdb dropdb doc clean
