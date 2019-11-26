import pytest
from django.db import connection


# @pytest.fixture(autouse=True)
# def install_backend():
#     import django.db.backends
#     import iseries
#     django.db.backends.iseries = iseries


@pytest.mark.django_db
def test_something():
    assert True
