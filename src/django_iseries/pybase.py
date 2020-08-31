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
import platform
import re

# For checking django's version
from functools import partial
from typing import Optional

import sqlparse
from django.db import utils

from django_iseries import Database

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
SQLCODE_0530_REGEX = re.compile(r"^(\[.+] *){4}SQL0530.*")
SQLCODE_0910_REGEX = re.compile(r"^(\[.+] *){4}SQL0910.*")


class DatabaseWrapper:
    # Get new database connection for non persistance connection 
    def get_new_connection(self, kwargs):
        driver_name = 'iSeries Access ODBC Driver' if platform.system() == 'Windows' else 'IBM i Access ODBC Driver'
        if 'port' in kwargs and 'host' in kwargs:
            kwargs['dsn'] = f"DRIVER={{{driver_name}}};DATABASE=%s;UNICODESQL=1;XDYNAMIC=1;" \
                            f"PKG=A/DJANGO,2,0,0,1,512;" \
                            "SYSTEM=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s" % (
                kwargs.get('database'),
                kwargs.get('host'),
                kwargs.get('port'),
                kwargs.get('user'),
                kwargs.get('password')
            )
        else:
            kwargs['dsn'] = kwargs.get('database')

        if 'security' in kwargs:
            kwargs['dsn'] += "security=%s;" % (kwargs.get('security'))
            del kwargs['security']

        if 'sslclientkeystoredb' in kwargs:
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDB=%s;" % (kwargs.get('sslclientkeystoredb'))
            del kwargs['sslclientkeystoredb']

        if 'sslclientkeystoredbpassword' in kwargs:
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDBPASSWORD=%s;" % (kwargs.get('sslclientkeystoredbpassword'))
            del kwargs['sslclientkeystoredbpassword']

        if 'sslclientkeystash' in kwargs:
            kwargs['dsn'] += "SSLCLIENTKEYSTASH=%s;" % (kwargs.get('sslclientkeystash'))
            del kwargs['sslclientkeystash']

        if 'sslservercertificate' in kwargs:
            kwargs['dsn'] += "SSLSERVERCERTIFICATE=%s;" % (kwargs.get('sslservercertificate'))
            del kwargs['sslservercertificate']

        conn_options = {'autocommit': False}
        kwargs['conn_options'] = conn_options
        if 'options' in kwargs:
            kwargs.update(kwargs.get('options'))
            del kwargs['options']
        if 'port' in kwargs:
            del kwargs['port']

        currentschema = kwargs.pop('currentschema', None)

        dsn = kwargs.pop('dsn', '')

        connection = Database.connect(dsn, **kwargs)
        if currentschema:
            cursor = DB2CursorWrapper(connection)
            cursor.set_current_schema(currentschema)

        return connection

    def is_active(self, connection=None):
        return bool(connection.cursor())

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
            # pylint: disable=(no-member)
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
        self.execute(f'SET CURRENT_SCHEMA = {schema}')

    def close(self):
        """
        Django calls close() twice on some cursors, but pyodbc does not allow this.
        pyodbc deletes the 'connection' attribute when closing a cursor, so we check for that.

        In the unlikely event that this code prevents close() from being called, pyodbc will close
        the cursor automatically when it goes out of scope.
        """
        if getattr(self, 'connection', False):
            self.cursor.close()

    def execute(self, query, params=()):
        if params:
            query = self.convert_query(query)
            query, params = self._replace_placeholders_in_select_clause(params, query)
        result = self._wrap_execute(partial(self.cursor.execute, query, params))
        return result

    def executemany(self, query, param_list):
        if not param_list:
            # empty param_list means do nothing (execute the query zero times)
            return
        query = self.convert_query(query)
        result = self._wrap_execute(partial(self.cursor.executemany, query, param_list))
        return result

    def _wrap_execute(self, execute):
        try:
            result = execute()
        except Database.Error as e:
            # iaccess seems to be sending incorrect sqlstate for some errors
            # reraise "referential constraint violation" errors as IntegrityError
            if e.args[0] == 'HY000' and SQLCODE_0530_REGEX.match(e.args[1]):
                raise utils.IntegrityError(*e.args, execute.func, *execute.args)
            elif e.args[0] == 'HY000' and SQLCODE_0910_REGEX.match(e.args[1]):
                # file in use error (likely in the same transaction)
                query, _params, *_ = execute.args
                if query.startswith('ALTER TABLE') and 'RESTART WITH' in query:
                    raise utils.ProgrammingError(
                        *e.args,
                        execute.func,
                        execute.args,
                        "Db2 for iSeries cannot reset a table's primary key sequence during same "
                        "transaction as insert/update on that table"
                    )
            raise type(e)(*e.args, execute.func, execute.args)
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
        result = self.execute('SELECT IDENTITY_VAL_LOCAL() AS IDENTITY FROM SYSIBM.SYSDUMMY1')
        row = result.fetchone()
        return row[0]

    def quote_value(self, value):
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time, str)):
            return f"'{value}'"
        if isinstance(value, bool):
            return '1' if value else '0'
        return str(value)
