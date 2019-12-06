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
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi              |
# +--------------------------------------------------------------------------+



import datetime
import re
import sys
import warnings

# For checking django's version
from typing import Optional

import sqlparse
from django import VERSION as djangoVersion
from django.conf import settings
from django.db import utils
from django.utils import six
from django.utils import timezone

from . import Database

dbms_name = 'dbms_name'

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
Error = Database.Error
InterfaceError = Database.InterfaceError
DataError = Database.DataError
OperationalError = Database.OperationalError
InternalError = Database.InternalError
ProgrammingError = Database.ProgrammingError
NotSupportedError = Database.NotSupportedError


FORMAT_QMARK_REGEX = re.compile(r'(?<!%)%s')
SQLCODE_0530_REGEX = re.compile("^(\[.+\] *){4}SQL0530.*")
SQLCODE_0910_REGEX = re.compile("^(\[.+\] *){4}SQL0910.*")


class DatabaseWrapper:
    # Get new database connection for non persistance connection 
    def get_new_connection(self, kwargs):
        kwargsKeys = kwargs.keys()
        if 'port' in kwargsKeys and 'host' in kwargsKeys:
            kwargs['dsn'] = "DRIVER={iSeries Access ODBC Driver};DATABASE=%s;" \
                            "SYSTEM=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s" % (
                kwargs.get('database'),
                kwargs.get('host'),
                kwargs.get('port'),
                kwargs.get('user'),
                kwargs.get('password')
            )
        else:
            kwargs['dsn'] = kwargs.get('database')

        if 'security' in kwargsKeys:
            kwargs['dsn'] += "security=%s;" % (kwargs.get('security'))
            del kwargs['security']

        if 'sslclientkeystoredb' in kwargsKeys:
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDB=%s;" % (kwargs.get('sslclientkeystoredb'))
            del kwargs['sslclientkeystoredb']

        if 'sslclientkeystoredbpassword' in kwargsKeys:
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDBPASSWORD=%s;" % (kwargs.get('sslclientkeystoredbpassword'))
            del kwargs['sslclientkeystoredbpassword']

        if 'sslclientkeystash' in kwargsKeys:
            kwargs['dsn'] += "SSLCLIENTKEYSTASH=%s;" % (kwargs.get('sslclientkeystash'))
            del kwargs['sslclientkeystash']

        if 'sslservercertificate' in kwargsKeys:
            kwargs['dsn'] += "SSLSERVERCERTIFICATE=%s;" % (kwargs.get('sslservercertificate'))
            del kwargs['sslservercertificate']

        conn_options = {'autocommit': False}
        kwargs['conn_options'] = conn_options
        if 'options' in kwargsKeys:
            kwargs.update(kwargs.get('options'))
            del kwargs['options']
        if 'port' in kwargsKeys:
            del kwargs['port']

        currentschema = kwargs.pop('currentschema', None)

        dsn = kwargs.pop('dsn', '')

        connection = Database.connect(dsn, **kwargs)
        if currentschema:
            cursor = DB2CursorWrapper(connection)
            cursor.set_current_schema(currentschema)

        return connection

    def is_active(self, connection=None):
        return Database.ibm_db.active(connection.conn_handler)

    # Over-riding _cursor method to return DB2 cursor.
    def _cursor(self, connection):
        return DB2CursorWrapper(connection)

    def close(self, connection):
        try:
            connection.close()
        except ProgrammingError as e:
            if str(e) == 'Attempt to use a closed connection.':
                pass
            else:
                raise

    def get_server_version(self, connection):
        self.connection = connection
        if not self.connection:
            self.cursor()
        return tuple(int(version) for version in self.connection.server_info()[1].split("."))


class DB2CursorWrapper:
    """
    This is the wrapper around IBM_DB_DBI in order to support format parameter style
    IBM_DB_DBI supports qmark, where as Django support format style, 
    hence this conversion is required.

    pyodbc.Cursor cannot be subclassed, so we store it as an attribute
    """

    def __init__(self, connection):
        self.cursor: Database.Cursor = connection.cursor()

    def __iter__(self):
        return self.cursor

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def get_current_schema(self):
        self.execute('select CURRENT_SCHEMA from sysibm.sysdummy1')
        return self.fetchone()[0]

    def set_current_schema(self, schema):
        self.execute(f'set CURRENT_SCHEMA = {schema}')

    def close(self):
        """
        Django calls close() twice on some cursors, but pyodbc does not allow this.
        pyodbc deletes the 'connection' attribute when closing a cursor, so we check for that.

        In the unlikely event that this code prevents close() from being called, pyodbc will close
        the cursor automatically when it goes out of scope.
        """
        if getattr(self, 'connection', False):
            self.cursor.close()

    def execute(self, query, params=None):
        if params is None:
            return self.cursor.execute(query)
        query = self.convert_query(query)
        query, params = self._replace_placeholders_in_select_clause(params, query)
        try:
            result = self.cursor.execute(query, params)
        except Database.Error as e:
            # iaccess seems to be sending incorrect sqlstate for some errors
            # reraise "referential constraint violation" errors as IntegrityError
            if e.args[0] == 'HY000' and SQLCODE_0530_REGEX.match(e.args[1]):
                raise utils.IntegrityError(*e.args)
            elif e.args[0] == 'HY000' and SQLCODE_0910_REGEX.match(e.args[1]):
                # file in use error (likely in the same transaction)
                if query.startswith('ALTER TABLE') and 'RESTART WITH' in query:
                    raise utils.ProgrammingError(
                        *e.args,
                        "Db2 for iSeries cannot reset a table's primary key sequence during same "
                        "transaction as insert/update on that table"
                    )
            raise
        if result == self.cursor:
            return self
        return result

    def executemany(self, query, param_list):
        if not param_list:
            # empty param_list means do nothing (execute the query zero times)
            return
        query = self.convert_query(query)
        try:
            result = self.cursor.executemany(query, param_list)
        except Database.Error as e:
            # iaccess seems to be sending incorrect sqlstate for some errors
            # reraise "referential constraint violation" errors as IntegrityError
            if e.args[0] == 'HY000' and SQLCODE_0530_REGEX.match(e.args[1]):
                raise utils.IntegrityError(*e.args)
            elif e.args[0] == 'HY000' and SQLCODE_0910_REGEX.match(e.args[1]):
                # file in use error (likely in the same transaction)
                if query.startswith('ALTER TABLE') and 'RESTART WITH' in query:
                    raise utils.ProgrammingError(
                        *e.args,
                        "Db2 for iSeries cannot reset a table's primary key sequence during same "
                        "transaction as insert/update on that table"
                    )
            raise
        if result == self.cursor:
            return self
        return result

    def _replace_placeholders_in_select_clause(self, params, query):
        """Db2 for i does not allow placeholders in select clause; this converts them to literals"""
        if isinstance(params, tuple):
            params = list(params)
        current_param_idx = -1
        in_select_clause = False
        tmp = []
        parsed_statement = sqlparse.parse(query)[0]
        for t in parsed_statement.flatten():
            if t.ttype == sqlparse.tokens.Name.Placeholder:  # '?'
                current_param_idx += 1
                if in_select_clause:
                    quoted_val = self.quote_value(params.pop(current_param_idx))
                    t = sqlparse.sql.Token(sqlparse.tokens.String, quoted_val)
            elif t.normalized == 'SELECT':
                in_select_clause = True
            elif t.normalized == 'FROM':
                in_select_clause = False
            tmp.append(str(t))
        query = ''.join(tmp)
        return query, params

    def convert_query(self, query):
        """
        Django uses "format" style placeholders, but the iaccess odbc driver uses "qmark" style.
        This fixes it -- but note that if you want to use a literal "%s" in a query,
        you'll need to use "%%s".
        """
        return FORMAT_QMARK_REGEX.sub('?', query).replace('%%', '%')

    def _row_factory(self, row: Optional[Database.Row]):
        if row is None:
            return row
        return tuple(row)

    def fetchone(self):
        return self._row_factory(self.cursor.fetchone())

    def fetchmany(self, size):
        return [self._row_factory(row) for row in self.cursor.fetchmany(size)]

    def fetchall(self):
        return [self._row_factory(row) for row in self.cursor.fetchall()]

    @property
    def last_identity_val(self):
        result = self.execute('select IDENTITY_VAL_LOCAL() as identity from sysibm.sysdummy1');
        row = result.fetchone()
        return row[0]

    def quote_value(self, value):
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time, str)):
            return f"'{value}'"
        if isinstance(value, bool):
            return '1' if value else '0'
        return str(value)

    #
    # def next(self):
    #     row = self.fetchone()
    #     if row == None:
    #         raise StopIteration
    #     return row
    #
    # def _create_instance(self, connection):
    #     return DB2CursorWrapper(connection)
    #
    # def _format_parameters(self, parameters):
    #     parameters = list(parameters)
    #     for index in range(len(parameters)):
    #         # With raw SQL queries, datetimes can reach this function
    #         # without being converted by DateTimeField.get_db_prep_value.
    #         if settings.USE_TZ and isinstance(parameters[index], datetime.datetime):
    #             param = parameters[index]
    #             if timezone.is_naive(param):
    #                 warnings.warn(u"Received a naive datetime (%s)"
    #                               u" while time zone support is active." % param,
    #                               RuntimeWarning)
    #                 default_timezone = timezone.get_default_timezone()
    #                 param = timezone.make_aware(param, default_timezone)
    #             param = param.astimezone(timezone.utc).replace(tzinfo=None)
    #             parameters[index] = param
    #     return tuple(parameters)
    #
    # # Over-riding this method to modify SQLs which contains format parameter to qmark.
    # def execute(self, operation, parameters=()):
    #     if (djangoVersion[0:2] >= (2, 0)):
    #         operation = str(operation)
    #     try:
    #         if operation.find('ALTER TABLE') == 0 and getattr(self.connection, dbms_name) != 'DB2':
    #             doReorg = 1
    #         else:
    #             doReorg = 0
    #         if operation.count("db2regexExtraField(%s)") > 0:
    #             operation = operation.replace("db2regexExtraField(%s)", "")
    #             operation = operation % parameters
    #             parameters = ()
    #         if operation.count("%s") > 0:
    #             operation = operation % (tuple("?" * operation.count("%s")))
    #         parameters = self._format_parameters(parameters)
    #
    #         try:
    #             if (doReorg == 1):
    #                 super(DB2CursorWrapper, self).execute(operation, parameters)
    #                 return self._reorg_tables()
    #             else:
    #                 return super(DB2CursorWrapper, self).execute(operation, parameters)
    #         except IntegrityError as e:
    #             six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(six.PY3 and e.args or (e._message,))),
    #                         sys.exc_info()[2])
    #             raise
    #
    #         except ProgrammingError as e:
    #             six.reraise(utils.ProgrammingError, utils.ProgrammingError(*tuple(six.PY3 and e.args or (e._message,))),
    #                         sys.exc_info()[2])
    #             raise
    #         except DatabaseError as e:
    #             six.reraise(utils.DatabaseError, utils.DatabaseError(*tuple(six.PY3 and e.args or (e._message,))),
    #                         sys.exc_info()[2])
    #             raise
    #     except (TypeError):
    #         return None
    #
    # # Over-riding this method to modify SQLs which contains format parameter to qmark.
    # def executemany(self, operation, seq_parameters):
    #     try:
    #         if operation.count("db2regexExtraField(%s)") > 0:
    #             raise ValueError("Regex not supported in this operation")
    #         if operation.count("%s") > 0:
    #             operation = operation % (tuple("?" * operation.count("%s")))
    #         seq_parameters = [self._format_parameters(parameters) for parameters in seq_parameters]
    #
    #         try:
    #             return super(DB2CursorWrapper, self).executemany(operation, seq_parameters)
    #         except IntegrityError as e:
    #             six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(six.PY3 and e.args or (e._message,))),
    #                         sys.exc_info()[2])
    #             raise
    #         except DatabaseError as e:
    #             six.reraise(utils.DatabaseError, utils.DatabaseError(*tuple(six.PY3 and e.args or (e._message,))),
    #                         sys.exc_info()[2])
    #             raise
    #     except (IndexError, TypeError):
    #         return None
    #
    # # table reorganization method
    # def _reorg_tables(self):
    #     checkReorgSQL = "select tabschema, tabname from sysibmadm.admintabinfo where reorg_pending = 'Y'"
    #     res = []
    #     reorgSQLs = []
    #     parameters = ()
    #     super(DB2CursorWrapper, self).execute(checkReorgSQL, parameters)
    #     res = super(DB2CursorWrapper, self).fetchall()
    #     if res:
    #         for sName, tName in res:
    #             reorgSQL = '''CALL SYSPROC.ADMIN_CMD('REORG TABLE "%(sName)s"."%(tName)s"')''' % {
    #                 'sName': sName, 'tName': tName
    #             }
    #             reorgSQLs.append(reorgSQL)
    #         for sql in reorgSQLs:
    #             super(DB2CursorWrapper, self).execute(sql)
    #
    # # Over-riding this method to modify result set containing datetime and time zone support is active
    # def fetchone(self):
    #     row = super(DB2CursorWrapper, self).fetchone()
    #     if row is None:
    #         return row
    #     else:
    #         return self._fix_return_data(row)
    #
    # # Over-riding this method to modify result set containing datetime and time zone support is active
    # def fetchmany(self, size=0):
    #     rows = super(DB2CursorWrapper, self).fetchmany(size)
    #     if rows is None:
    #         return rows
    #     else:
    #         return [self._fix_return_data(row) for row in rows]
    #
    # # Over-riding this method to modify result set containing datetime and time zone support is active
    # def fetchall(self):
    #     rows = super(DB2CursorWrapper, self).fetchall()
    #     if rows is None:
    #         return rows
    #     else:
    #         return [self._fix_return_data(row) for row in rows]
    #
    # # This method to modify result set containing datetime and time zone support is active
    # def _fix_return_data(self, row):
    #     row = list(row)
    #     index = -1
    #     for value, desc in zip(row, self.description):
    #         index = index + 1
    #         if (desc[1] == Database.DATETIME):
    #             if settings.USE_TZ and value is not None and timezone.is_naive(value):
    #                 value = value.replace(tzinfo=timezone.utc)
    #                 row[index] = value
    #         else:
    #             if isinstance(value, six.string_types):
    #                 row[index] = re.sub(r'[\x00]', '', value)
    #     return tuple(row)
