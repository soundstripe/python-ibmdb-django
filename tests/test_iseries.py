import pytest
from django.core.exceptions import ImproperlyConfigured

from iseries.creation import DatabaseCreation


@pytest.mark.django_db
def test_something():
    assert True


@pytest.fixture
def connection():
    from django.db import connection
    new_connection = connection.copy()
    yield new_connection
    new_connection.close()


@pytest.fixture
def editor(connection):
    return connection.schema_editor()


@pytest.mark.parametrize('value,expected', [
    ('string', "'string'"),
    (42, '42'),
    (1.754, '1.754'),
    (False, '0'),
])
def test_quote_value(editor, value, expected):
    actual = editor.quote_value(value)
    assert expected == actual


# noinspection PyProtectedMember
def test_create_test_db(connection):
    creation = DatabaseCreation(connection)
    with pytest.raises(ImproperlyConfigured):
        # Db2 for iSeries does not support creating new databases
        creation._create_test_db(verbosity=0, autoclobber=False, keepdb=False)
    creation._create_test_db(verbosity=0, autoclobber=False, keepdb=True)
