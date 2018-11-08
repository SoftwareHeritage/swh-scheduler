# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-scheduler-updater-dev
DOCDIR = autodoc

SQL_INIT  = 10-swh-init.sql
SQL_SCHEMA  = 30-swh-schema.sql
SQL_FUNC  = 40-swh-func.sql
SQLS = $(SQL_INIT) $(SQL_SCHEMA) $(SQL_FUNC)
SQL_FILES = $(abspath $(addprefix \
  $(CURDIR)/../../../swh/scheduler/sql/updater/,$(SQLS)))

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

dumpdb: swh-scheduler-updater.dump
swh-scheduler-updater.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

$(DOCDIR):
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)

doc: autodoc-stamp $(DOCDIR)/swh-scheduler-updater.pdf
autodoc-stamp: filldb-stamp $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh-scheduler-updater
	cp -a $(DOCDIR)/swh-scheduler-updater.dot $(DOCDIR)/swh-scheduler-updater.dot.orig
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

$(DOCDIR)/swh-scheduler-updater.pdf: $(DOCDIR)/swh-scheduler-updater.dot autodoc-stamp
	dot -T pdf $< > $@
$(DOCDIR)/swh-scheduler-updater.svg: $(DOCDIR)/swh-scheduler-updater.dot autodoc-stamp
	dot -T svg $< > $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh-scheduler-updater.dump

.PHONY: all initdb createdb dropdb doc clean
