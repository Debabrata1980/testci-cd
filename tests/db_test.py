"""
This file illustrates a few examples of using pgmock with pytest.

A postgres testing database from pytest-pgsql (https://github.com/CloverHealth/pytest-pgsql)
is used and a fixture is created for using the mock context manager. This is the
preferred way of using pgmock, but it's also possible to render SQL yourself and execute
patched versions of it. Examples of this are also included here
"""
import pytest

import pgmock

import testing.postgresql
import sqlalchemy
import pgmock.exceptions

test_db = testing.postgresql.Postgresql()
test_engine = sqlalchemy.create_engine(test_db.url())

query = '''SELECT * from table1;
SELECT * from table2;
SELECT * from table3
'''

# Obtain the first statement
print(pgmock.sql(query, pgmock.statement(0)))

# Obtain the first three statements using a range of 0 - 3 (3 is the exclusive ending index)
print(pgmock.sql(query, pgmock.statement(0, 3)))

# Create a query and filter a column
query = "SELECT c2 FROM my_table WHERE c1 = 'value'"

# Create a patch for the table. The patch takes a selector,
# rows (a list of lists for each column or a list of dictionaries keyed on column),
# and column names
patch = pgmock.patch(pgmock.table('my_table'),
                     [('dummy_data', 'data'), ('value', 'hello'), ('value', 'hi')],
                     ['c1', 'c2'])

# Render the patched SQL so that we can execute it
sql = pgmock.sql(query, patch)
print(sql)

# Execute the SQL and verify that filtering happened correctly
results = list(test_engine.execute(sql))
assert results == [('hello',), ('hi',)]

'''
@pytest.fixture
def pgmocker(transacted_postgresql_db):
    with pgmock.mock(transacted_postgresql_db.connection) as mocker:
        yield mocker


def test_table_patching_w_mocker(transacted_postgresql_db, pgmocker):
    """Tests patching a table while using the mocker returned by ``pgmock.mock``"""
    pgmocker.patch(pgmock.table('test_table'), [('val1', 'val2'), ('val3', 'val4')], ['c1', 'c2'])

    results = list(transacted_postgresql_db.connection.execute('SELECT * from test_table'))
    assert results == [('val1', 'val2'), ('val3', 'val4')]


def test_patch_subquery_from_file(transacted_postgresql_db, tmpdir):
    """Tests reading a subquery from a file and testing a patched version of it"""
    # Create the example file
    file_name = tmpdir.join('file.sql')
    file_name.write('SELECT sub.c1, sub.c2 FROM (SELECT * FROM test_table) sub;')

    # Read the subquery 'sub' from the file
    subquery = pgmock.sql_file(str(file_name), pgmock.subquery('sub'))
    assert subquery == 'SELECT * FROM test_table'

    # Patch the table of the subquery and verify it returns the proper results
    patched = pgmock.sql(subquery, pgmock.patch(
        pgmock.table('test_table'),
        rows=[('v1', 'v2'), ('v3', 'v4')],
        cols=['c1', 'c2']
    ))
    assert (
        patched == "SELECT * FROM  (VALUES ('v1','v2'),('v3','v4')) AS test_table(\"c1\",\"c2\")"
    )

    # Patches can also be applied with list of dictionaries, filling in only what's needed.
    # Column names must still be provided. null values will be filled for all missing columns
    patched = pgmock.sql(subquery, pgmock.patch(
        pgmock.table('test_table'),
        rows=[{'c1': 'v1'}, {'c2': 'v4'}],
        cols=['c1', 'c2']
    ))
    assert (
        patched == "SELECT * FROM  (VALUES ('v1',null),(null,'v4')) AS test_table(\"c1\",\"c2\")"
    )

    results = list(transacted_postgresql_db.connection.execute(patched))
    assert results == [('v1', None), (None, 'v4')]
 '''