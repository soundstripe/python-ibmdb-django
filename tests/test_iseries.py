import pytest
from django.core.exceptions import ImproperlyConfigured

from django_iseries.creation import DatabaseCreation
from tests.models import Object, ObjectReference


@pytest.mark.django_db
def test_sanity_check():
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


class DBConstraintTests:
    @pytest.mark.django_db
    def test_can_reference_existent(self):
        obj = Object.objects.create()
        ref = ObjectReference.objects.create(obj=obj)
        assert ref.obj == obj

        ref = ObjectReference.objects.get(obj=obj)
        assert ref.obj == obj

    @pytest.mark.django_db
    def test_can_reference_non_existent(self):
        assert not Object.objects.filter(id=12345).exists()
        ref = ObjectReference.objects.create(obj_id=12345)
        ref_new = ObjectReference.objects.get(obj_id=12345)
        assert ref == ref_new

        # noinspection PyTypeChecker
        with pytest.raises(Object.DoesNotExist):
            # noinspection PyStatementEffect
            ref.obj

    @pytest.mark.django_db
    def test_many_to_many(self):
        obj = Object.objects.create()
        obj.related_objects.create()
        assert Object.objects.count() == 2
        assert obj.related_objects.count() == 1

        intermediary_model = Object._meta.get_field("related_objects").remote_field.through
        intermediary_model.objects.create(from_object_id=obj.id, to_object_id=12345)
        assert obj.related_objects.count() == 1
        assert intermediary_model.objects.count() == 2
