# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2018.                                      |
# +--------------------------------------------------------------------------+
# | This module complies with Django 1.0 and is                              |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi,             |
# | Hemlata Bhatt, Vyshakh A                                                 |
# +--------------------------------------------------------------------------+

import datetime
import uuid
from functools import lru_cache

import pytz
from django.conf import settings
from django.db import utils
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.timezone import is_aware, utc

from iseries import query

dbms_name = 'dbms_name'


class DatabaseOperations(BaseDatabaseOperations):
    def __init__(self, connection):
        super(DatabaseOperations, self).__init__(self)
        self.connection = connection

    compiler_module = "iseries.compiler"

    def cache_key_culling_sql(self):
        return '''select cache_key 
                    from (select cache_key, ( ROW_NUMBER() over() ) as rownum from %s order by cache_key)
                    where rownum = %%s + 1
        '''

    def check_aggregate_support(self, aggregate):
        # In DB2 data type of the result is the same as the data type of the argument values for AVG aggregation
        # But Django aspect in Float regardless of data types of argument value
        # http://publib.boulder.ibm.com/infocenter/db2luw/v9r7/index.jsp?topic=/com.ibm.db2.luw.apdv.cli.doc/doc/c0007645.html
        if aggregate.sql_function == 'AVG':
            aggregate.sql_template = '%(function)s(DOUBLE(%(field)s))'
        # In DB2 equivalent sql function of STDDEV_POP is STDDEV
        elif aggregate.sql_function == 'STDDEV_POP':
            aggregate.sql_function = 'STDDEV'
        # In DB2 equivalent sql function of VAR_SAMP is VARIENCE
        elif aggregate.sql_function == 'VAR_POP':
            aggregate.sql_function = 'VARIANCE'
        # DB2 doesn't have sample standard deviation function
        elif aggregate.sql_function == 'STDDEV_SAMP':
            raise NotImplementedError("sample standard deviation function not supported")
        # DB2 doesn't have sample variance function
        elif aggregate.sql_function == 'VAR_SAMP':
            raise NotImplementedError("sample variance function not supported")

    def get_db_converters(self, expression):
        """
        Get a list of functions needed to convert field data.
        """
        converters = super(DatabaseOperations, self).get_db_converters(expression)

        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        return converters

    def combine_expression(self, connector, sub_expressions):
        lhs, rhs = sub_expressions
        if connector == '%%':
            return f'MOD({lhs}, {rhs})'
        elif connector == '&':
            return f'BITAND({lhs}, {rhs})'
        elif connector == '|':
            return f'BITOR({lhs}, {rhs})'
        elif connector == '<<':
            return f'({lhs} * POWER(2, CAST({rhs} AS INTEGER)))'
        elif connector == '>>':
            return f'FLOOR({lhs} / POWER(2, CAST({rhs} AS INTEGER)))'
        elif connector == '^':
            return f'POWER({lhs}, {rhs})'
        return super().combine_expression(connector, sub_expressions)
        # elif connector == '-':
        #     strr = str(sub_expressions[1])
        #     sub_expressions[1] = strr.replace('+', '-')
        #     return super(DatabaseOperations, self).combine_expression(connector, sub_expressions)
        # else:
        #     strr = str(sub_expressions[1])
        #     sub_expressions[1] = strr.replace('+', '-')
        #     return super(DatabaseOperations, self).combine_expression(connector, sub_expressions)

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def format_for_duration_arithmetic(self, sql):
        return ' %s MICROSECONDS' % sql

    # Function to extract day, month or year from the date.
    # Reference: http://publib.boulder.ibm.com/infocenter/db2luw/v9r5/topic/com.ibm.db2.luw.sql.ref.doc/doc/r0023457.html
    def date_extract_sql(self, lookup_type, field_name):
        if lookup_type.upper() == 'WEEK_DAY':
            return " DAYOFWEEK(%s) " % (field_name)
        else:
            return " %s(%s) " % (lookup_type.upper(), field_name)

    def _get_utcoffset(self, tzname):
        if pytz is None and tzname is not None:
            utils.NotSupportedError("Not supported without pytz")
        else:
            hr = 0
            min = 0
            tz = pytz.timezone(tzname)
            td = tz.utcoffset(datetime.datetime(2012, 1, 1))
            if td.days is -1:
                min = (td.seconds % (60 * 60)) / 60 - 60
                if min:
                    hr = td.seconds / (60 * 60) - 23
                else:
                    hr = td.seconds / (60 * 60) - 24
            else:
                hr = td.seconds / (60 * 60)
                min = (td.seconds % (60 * 60)) / 60
            return hr, min

    def adapt_timefield_value(self, value):
        """
        Transform a time value to an object compatible with what is expected
        by the backend driver for time columns.
        """
        if value is None:
            return None
        if timezone.is_aware(value):
            raise ValueError("Db2 for iSeries backend does not support timezone-aware times.")
        return value

    def adapt_datetimefield_value(self, value):
        """
        Transform a time value to an object compatible with what is expected
        by the backend driver for time columns.
        """
        if value is None:
            return None
        if timezone.is_aware(value):
            raise ValueError("Db2 for iSeries backend does not support timezone-aware times.")
        return value

    # Function to extract time zone-aware day, month or day of week from timestamps   
    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        if settings.USE_TZ:
            hr, min = self._get_utcoffset(tzname)
            if hr < 0:
                field_name = "%s - %s HOURS - %s MINUTES" % (field_name, -hr, -min)
            else:
                field_name = "%s + %s HOURS + %s MINUTES" % (field_name, hr, min)

        if lookup_type.upper() == 'WEEK_DAY':
            return " DAYOFWEEK(%s) " % (field_name)
        else:
            return " %s(%s) " % (lookup_type.upper(), field_name)

    # Truncating the date value on the basic of lookup type.
    # e.g If input is 2008-12-04 and month then output will be 2008-12-01 00:00:00
    # Reference: http://www.ibm.com/developerworks/data/library/samples/db2/0205udfs/index.html
    def date_trunc_sql(self, lookup_type, field_name):
        sql = "TIMESTAMP(DATE(SUBSTR(CHAR(%s), 1, %d) || '%s'), TIME('00:00:00'))"
        if lookup_type.upper() == 'DAY':
            sql = sql % (field_name, 10, '')
        elif lookup_type.upper() == 'MONTH':
            sql = sql % (field_name, 7, '-01')
        elif lookup_type.upper() == 'YEAR':
            sql = sql % (field_name, 4, '-01-01')
        return sql

    # Truncating the time zone-aware timestamps value on the basic of lookup type
    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        sql = "TIMESTAMP(SUBSTR(CHAR(%s), 1, %d) || '%s')"
        if settings.USE_TZ:
            hr, min = self._get_utcoffset(tzname)
            if hr < 0:
                field_name = "%s - %s HOURS - %s MINUTES" % (field_name, -hr, -min)
            else:
                field_name = "%s + %s HOURS + %s MINUTES" % (field_name, hr, min)
        if lookup_type.upper() == 'SECOND':
            sql = sql % (field_name, 19, '.000000')
        if lookup_type.upper() == 'MINUTE':
            sql = sql % (field_name, 16, '.00.000000')
        elif lookup_type.upper() == 'HOUR':
            sql = sql % (field_name, 13, '.00.00.000000')
        elif lookup_type.upper() == 'DAY':
            sql = sql % (field_name, 10, '-00.00.00.000000')
        elif lookup_type.upper() == 'MONTH':
            sql = sql % (field_name, 7, '-01-00.00.00.000000')
        elif lookup_type.upper() == 'YEAR':
            sql = sql % (field_name, 4, '-01-01-00.00.00.000000')
        return sql, []

    def date_interval_sql(self, timedelta):
        return " %d days + %d seconds + %d microseconds" % (
            timedelta.days, timedelta.seconds, timedelta.microseconds)

    # As casting is not required, so nothing is required to do in this function.
    def datetime_cast_sql(self):
        return "%s"

    # Function to return SQL from dropping foreign key.
    def drop_foreignkey_sql(self):
        return "DROP FOREIGN KEY"

    # Dropping auto generated property of the identity column.
    def drop_sequence_sql(self, table):
        return "ALTER TABLE %s ALTER COLUMN ID DROP IDENTITY" % (self.quote_name(table))

    def fulltext_search_sql(self, field_name):
        sql = "WHERE %s = ?" % field_name
        return sql

    # Function to return value of auto-generated field of last executed insert query. 
    def last_insert_id(self, cursor, table_name, pk_name):
        result = cursor.execute('SELECT IDENTITY_VAL_LOCAL() AS IDENTITY FROM SYSIBM.SYSDUMMY1')
        return result.fetchone()[0]

    # In case of WHERE clause, if the search is required to be case insensitive then converting 
    # left hand side field to upper.
    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    # As DB2 v91 specifications, 
    # Maximum length of a table name and Maximum length of a column name is 128
    # http://publib.boulder.ibm.com/infocenter/db2e/v9r1/index.jsp?topic=/com.ibm.db2e.doc/db2elimits.html
    def max_name_length(self):
        return 128

    # As DB2 v97 specifications,
    # Maximum length of a database name is 8
    # http://publib.boulder.ibm.com/infocenter/db2luw/v9r7/topic/com.ibm.db2.luw.sql.ref.doc/doc/r0001029.html
    def max_db_name_length(self):
        return 8

    def no_limit_value(self):
        return None

    # Method to point custom query class implementation.
    def query_class(self, DefaultQueryClass):
        return query.query_class(DefaultQueryClass)

    # Function to quote the name of schema, table and column.
    def quote_name(self, name=None):
        name = name.upper()
        if (name.startswith("\"") & name.endswith("\"")):
            return name

        if (name.startswith("\"")):
            return "%s\"" % name

        if (name.endswith("\"")):
            return "\"%s" % name

        return "\"%s\"" % name

    # SQL to return RANDOM number.
    # Reference: http://publib.boulder.ibm.com/infocenter/db2luw/v8/topic/com.ibm.db2.udb.doc/admin/r0000840.htm
    def random_function_sql(self):
        return "SYSFUN.RAND()"

    def regex_lookup(self, lookup_type):
        if lookup_type == 'regex':
            return '''xmlcast( xmlquery('fn:matches(xs:string($c), "%%s")' passing %s as "c") as varchar(5)) = 'true' db2regexExtraField(%s)'''
        else:
            return '''xmlcast( xmlquery('fn:matches(xs:string($c), "%%s", "i")' passing %s as "c") as varchar(5)) = 'true' db2regexExtraField(%s)'''

    # As save-point is supported by DB2, following function will return SQL to create savepoint.
    def savepoint_create_sql(self, sid):
        return "SAVEPOINT %s ON ROLLBACK RETAIN CURSORS" % sid

    # Function to commit savepoint.   
    def savepoint_commit_sql(self, sid):
        return "RELEASE TO SAVEPOINT %s" % sid

    # Function to rollback savepoint.
    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO SAVEPOINT %s" % sid

    # Deleting all the rows from the list of tables provided and resetting all the
    # sequences.
    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            truncated_tables = set(t.upper() for t in tables)
            constraints = set()
            for table in tables:
                for foreign_table, constraint, foreign_pk_col, foreign_tgt_col in self._foreign_key_constraints(table):
                    if allow_cascade:
                        truncated_tables.add(foreign_table)
                    constraints.add((foreign_table, constraint, foreign_pk_col, foreign_tgt_col, table))

            sqls = [self._drop_constraint_sql(*constraint) for constraint in constraints]
            sqls.append('COMMIT')

            sqls.extend(' '.join([style.SQL_KEYWORD("DELETE"),
                                     style.SQL_KEYWORD("FROM"),
                                     style.SQL_TABLE("%s" % self.quote_name(table)),
                                     style.SQL_KEYWORD("WHERE"),
                                     "1 = 1"]) for table in tables)
            sqls.append('COMMIT')

            sqls.extend(' '.join([style.SQL_KEYWORD("ALTER TABLE"),
                                  style.SQL_TABLE("%s" % self.quote_name(sequence['table'])),
                                  style.SQL_KEYWORD("ALTER COLUMN"),
                                  self.quote_name(sequence['column']),
                                  style.SQL_KEYWORD("RESTART WITH 1")])
                        for sequence in sequences
                        if sequence['column'] is not None)

            sqls.extend(self._add_constraint_sql(*constraint) for constraint in constraints)
            return sqls
        else:
            return []

    def sequence_reset_sql(self, style, model_list):
        """
        Note: Db2 for iSeries cannot reset a table's primary key sequence if you have an open transaction
        that has already inserted/updated that table.
        """
        from django.db import models
        cursor = self.connection.cursor()
        sqls = []
        for model in model_list:
            table = model._meta.db_table
            for field in model._meta.local_fields:
                if isinstance(field, models.AutoField):
                    max_sql = "SELECT MAX(%s) FROM %s" % (self.quote_name(field.column), self.quote_name(table))
                    cursor.execute(max_sql)
                    max_id = [row[0] for row in cursor.fetchall()]
                    if max_id[0] == None:
                        max_id[0] = 0
                    sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                                style.SQL_TABLE("%s" % self.quote_name(table)) +
                                " " + style.SQL_KEYWORD("ALTER COLUMN") + " %s "
                                % self.quote_name(field.column) +
                                style.SQL_KEYWORD("RESTART WITH %s" % (max_id[0] + 1)))
                    break

            for field in model._meta.many_to_many:
                m2m_table = field.m2m_db_table()
                if field.remote_field is not None and hasattr(field.remote_field, 'through'):
                    flag = field.remote_field.through
                else:
                    flag = False
                if not flag:
                    max_sql = "SELECT MAX(%s) FROM %s" % (self.quote_name('ID'), self.quote_name(table))
                    cursor.execute(max_sql)
                    max_id = [row[0] for row in cursor.fetchall()]
                    if max_id[0] == None:
                        max_id[0] = 0
                    sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                                style.SQL_TABLE("%s" % self.quote_name(m2m_table)) +
                                " " + style.SQL_KEYWORD("ALTER COLUMN") + " %s "
                                % self.quote_name('ID') +
                                style.SQL_KEYWORD("RESTART WITH %s" % (max_id[0] + 1)))
        if cursor:
            cursor.close()

        return sqls

    # Returns sqls to reset the passed sequences
    def sequence_reset_by_name_sql(self, style, sequences):
        sqls = []
        for seq in sequences:
            sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                        style.SQL_TABLE("%s" % self.quote_name(seq.get('table'))) +
                        " " + style.SQL_KEYWORD("ALTER COLUMN") + " %s " % self.quote_name(seq.get('column')) +
                        style.SQL_KEYWORD("RESTART WITH %s" % (1)))
        return sqls


    def value_to_db_datetime(self, value):
        if value is None:
            return None

        if is_aware(value):
            if settings.USE_TZ:
                value = value.astimezone(utc).replace(tzinfo=None)
            else:
                raise ValueError("Timezone aware datetime not supported")
        return str(value)

    def value_to_db_time(self, value):
        if value is None:
            return None

        if is_aware(value):
            raise ValueError("Timezone aware time not supported")
        else:
            return value

    def year_lookup_bounds_for_date_field(self, value):
        lower_bound = datetime.date(int(value), 1, 1)
        upper_bound = datetime.date(int(value), 12, 31)
        return [lower_bound, upper_bound]

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join(f'({sql})' for sql in placeholder_rows_sql)
        return f"VALUES {values_sql}"

    def for_update_sql(self, nowait=False, skip_locked=False, of=()):
        # DB2 doesn't support nowait select for update
        if nowait:
            raise utils.DatabaseError("Nowait Select for update not supported ")
        else:
            return 'WITH RS USE AND KEEP UPDATE LOCKS'

    def fetch_returned_insert_id(self, cursor):
        return cursor.fetchone()[0]

    def fetch_returned_insert_ids(self, cursor):
        return [id_ for (id_, ) in cursor.fetchall()]

    def return_insert_id(self):
        """empty implementation as we implement returned ids with a cursor and custom Insert compiler"""
        return None, None

    def __foreign_key_constraints(self, table):
        foreign_keys_sql = """
        SELECT FK.TABLE_NAME, CST.CONSTRAINT_NAME, FK.COLUMN_NAME, TGT.COLUMN_NAME
            FROM QSYS2.SYSCST CST
                JOIN QSYS2.SYSKEYCST FK 
                  ON CST.CONSTRAINT_SCHEMA = FK.CONSTRAINT_SCHEMA 
                 AND CST.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
                JOIN QSYS2.SYSREFCST REF 
                  ON CST.CONSTRAINT_SCHEMA = REF.CONSTRAINT_SCHEMA 
                 AND CST.CONSTRAINT_NAME = REF.CONSTRAINT_NAME
                JOIN QSYS2.SYSKEYCST PK 
                  ON REF.UNIQUE_CONSTRAINT_SCHEMA = PK.CONSTRAINT_SCHEMA 
                 AND REF.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
                JOIN QSYS2.SYSCOLUMNS TGT
                  ON PK.TABLE_SCHEMA = TGT.TABLE_SCHEMA
                 AND PK.TABLE_NAME = TGT.TABLE_NAME
                 AND TGT.ORDINAL_POSITION = FK.COLUMN_POSITION
            WHERE CST.CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND FK.ORDINAL_POSITION = PK.ORDINAL_POSITION
              AND PK.TABLE_SCHEMA = CURRENT_SCHEMA
              AND PK.TABLE_NAME = ?
              AND ENABLED = 'YES'
          """.strip()
        cursor = self.connection.cursor()
        foreign_keys = cursor.execute(foreign_keys_sql, [table.upper()]).fetchall()
        return foreign_keys

    @cached_property
    def _foreign_key_constraints(self):
        """cached implementation, modelled after django's oracle backend implementation"""
        return lru_cache(maxsize=512)(self.__foreign_key_constraints)

    def _drop_constraint_sql(self, referencing_table, constraint_name, referencing_col, target_col, target_table):
        t = self.quote_name(referencing_table)
        c = self.quote_name(constraint_name)
        drop_constraint_sql = f"ALTER TABLE {t} DROP CONSTRAINT {c}"
        return drop_constraint_sql

    def _add_constraint_sql(self, referencing_table, constraint_name, referencing_col, target_col, target_table):
        ref_t = self.quote_name(referencing_table)
        cst = self.quote_name(constraint_name)
        col = self.quote_name(referencing_col)
        target_t = self.quote_name(target_table)
        add_constraint_sql = f"ALTER TABLE {ref_t} ADD CONSTRAINT {cst} " \
                             f"FOREIGN KEY ({col}) REFERENCES {target_t} ({target_col})"
        return add_constraint_sql