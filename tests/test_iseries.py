import pytest
from django.db import connection


# @pytest.fixture(autouse=True)
# def install_backend():
#     import django.db.backends
#     import iseries
#     django.db.backends.iseries = iseries


# @pytest.fixture(scope='session')
# def django_db_createdb():
#     return False
#
#
# @pytest.fixture(scope='session')
# def django_db_keepdb():
#     return True
#

@pytest.mark.django_db
def test_something():
    assert True
