# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-scheduler-updater-dev
DOCDIR = autodoc

SQL_INIT  = swh-init.sql
SQL_SCHEMA  = swh-schema.sql
SQL_FUNC  = swh-func.sql
SQLS = $(SQL_INIT) $(SQL_SCHEMA) $(SQL_FUNC)

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

dumpdb: swh-scheduler-updater.dump
swh-scheduler.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

doc: autodoc-stamp $(DOCDIR)/swh-scheduler-updater.pdf
autodoc-stamp: filldb-stamp
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh
	cp -a $(DOCDIR)/swh-scheduler.dot $(DOCDIR)/swh-scheduler-updater.dot.orig
	touch $@

$(DOCDIR)/swh-scheduler.pdf: autodoc-stamp
	dot -T pdf $(DOCDIR)/swh-scheduler-updater.dot > $(DOCDIR)/swh-scheduler-updater.pdf

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh-scheduler-updater.dump

.PHONY: all initdb createdb dropdb doc clean
