# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-scheduler-dev
DOCDIR = autodoc

SQL_SCHEMA  = swh-scheduler-schema.sql
SQL_DATA    = swh-scheduler-data.sql
SQLS = $(SQL_SCHEMA) $(SQL_DATA)

PSQL_BIN = psql
PSQL_FLAGS = --echo-all -X -v ON_ERROR_STOP=1
PSQL = $(PSQL_BIN) $(PSQL_FLAGS)

all:

createdb: createdb-stamp
createdb-stamp: $(SQL_INIT)
	createdb $(DBNAME)
	touch $@

filldb: filldb-stamp
filldb-stamp: createdb-stamp
	cat $(SQLS) | $(PSQL) $(DBNAME)
	touch $@

dropdb:
	-dropdb $(DBNAME)

dumpdb: swh-scheduler.dump
swh-scheduler.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

doc: autodoc-stamp $(DOCDIR)/swh-scheduler.pdf
autodoc-stamp: filldb-stamp
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh
	cp -a $(DOCDIR)/swh-scheduler.dot $(DOCDIR)/swh-scheduler.dot.orig
	touch $@

$(DOCDIR)/swh-scheduler.pdf: autodoc-stamp
	dot -T pdf $(DOCDIR)/swh-scheduler.dot > $(DOCDIR)/swh-scheduler.pdf

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh-scheduler.dump

.PHONY: all initdb createdb dropdb doc clean
