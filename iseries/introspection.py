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
from collections import namedtuple

from . import Database

try:
    from django.db.backends import BaseDatabaseIntrospection, FieldInfo
except ImportError:
    from django.db.backends.base.introspection import BaseDatabaseIntrospection, FieldInfo

TableInfo = namedtuple('TableInfo', ['name', 'type'])


class DatabaseIntrospection(BaseDatabaseIntrospection):
    """
    This is the class where database metadata information can be generated.
    """

    data_types_reverse = {
        Database.SQL_BIGINT: "BigIntegerField",
        Database.SQL_BINARY: "BinaryField",
        Database.SQL_CHAR: "CharField",
        Database.SQL_DECIMAL: "DecimalField",
        Database.SQL_DOUBLE: "FloatField",
        Database.SQL_FLOAT: "FloatField",
        Database.SQL_INTEGER: "IntegerField",
        Database.SQL_LONGVARBINARY: 'BinaryField',
        Database.SQL_LONGVARCHAR: 'TextField',
        Database.SQL_NUMERIC: "DecimalField",
        Database.SQL_SS_XML: "XMLField",
        Database.SQL_TYPE_DATE: "DateField",
        Database.SQL_TYPE_TIME: "TimeField",
        Database.SQL_TYPE_TIMESTAMP: "DateTimeField",
        Database.SQL_VARBINARY: 'BinaryField',
        Database.SQL_VARCHAR: "TextField",
        Database.SQL_WCHAR: 'CharField',
        Database.SQL_WLONGVARCHAR: 'TextField',
        Database.SQL_WVARCHAR: 'TextField',
    }

    def get_field_type(self, data_type, description):
        return super(DatabaseIntrospection, self).get_field_type(data_type, description)

    # Getting the list of all tables, which are present under current schema.
    def get_table_list(self, cursor):
        table_query = "SELECT TABLE_NAME, LOWER(TABLE_TYPE) FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA = CURRENT_SCHEMA"
        tables = cursor.execute(table_query)
        return [TableInfo(self.identifier_converter(t_name), t_type) for t_name, t_type in tables]

    # Generating a dictionary for foreign key details, which are present under current schema.
    def get_relations(self, cursor, table_name):
        relations = {}
        schema = cursor.get_current_schema()
        for fk in cursor.foreignKeys(table=table_name, schema=schema):
            relations[self.__get_col_index(cursor, schema, table_name, fk['FKCOLUMN_NAME'])] = (
                self.__get_col_index(cursor, schema, fk['PKTABLE_NAME'], fk['PKCOLUMN_NAME']),
                fk['PKTABLE_NAME'].lower())
        return relations

    # Private method. Getting Index position of column by its name
    def __get_col_index(self, cursor, schema, table_name, col_name):
        for col in cursor.columns(schema, table_name, [col_name]):
            return col['ORDINAL_POSITION'] - 1

    def get_key_columns(self, cursor, table_name):
        relations = []
        schema = cursor.get_current_schema()
        for fk in cursor.foreignKeys(table=table_name, schema=schema):
            relations.append((fk['FKCOLUMN_NAME'].lower(), fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()))
        return relations

    # Getting the description of the table.
    def get_table_description(self, cursor, table_name):
        qn = self.connection.ops.quote_name
        description = []
        table_type = 'T'
        schema = cursor.get_current_schema()

        sql = "SELECT TYPE FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='%(schema)s' AND TABLE_NAME='%(table)s'" % {
            'schema': schema.upper(), 'table': table_name.upper()
        }
        cursor.execute(sql)
        table_type = cursor.fetchone()[0]

        if table_type != 'X':
            cursor.execute("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY" % qn(table_name))
            for desc in cursor.description:
                description.append([desc[0].lower(), ] + desc[1:])
        return description

    def get_constraints(self, cursor, table_name):
        constraints = {}
        schema = cursor.get_current_schema()

        sql = "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM QSYS2.SYSCSTCOL WHERE TABLE_SCHEMA='%(schema)s' AND TABLE_NAME='%(table)s'" % {
            'schema': schema.upper(), 'table': table_name.upper()
        }
        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': True,
                    'index': False
                }
            constraints[constname]['columns'].append(colname.lower())

        sql = "SELECT KEYCOL.CONSTRAINT_NAME, KEYCOL.COLUMN_NAME FROM QSYS2.SYSKEYCST KEYCOL INNER JOIN QSYS2.SYSCST TABCONST ON KEYCOL.CONSTRAINT_NAME=TABCONST.CONSTRAINT_NAME WHERE TABCONST.TABLE_SCHEMA='%(schema)s' and TABCONST.TABLE_NAME='%(table)s' and TABCONST.TYPE='U'" % {
            'schema': schema.upper(), 'table': table_name.upper()
        }
        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': True,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[constname]['columns'].append(colname.lower())

        for pkey in cursor.primaryKeys(schema=schema, table=table_name):
            if pkey['PK_NAME'] not in constraints:
                constraints[pkey['PK_NAME']] = {
                    'columns': [],
                    'primary_key': True,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[pkey['PK_NAME']]['columns'].append(pkey['COLUMN_NAME'].lower())

        for fk in cursor.foreignKeys(table=table_name, schema=schema):
            if fk['FK_NAME'] not in constraints:
                constraints[fk['FK_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                    'check': False,
                    'index': False
                }
            constraints[fk['FK_NAME']]['columns'].append(fk['FKCOLUMN_NAME'].lower())
            if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)

        sql = ("SELECT IDX.INDEX_NAME, K.COLUMN_NAME "
               "  FROM QSYS2.SYSINDEXES IDX "
               "  JOIN QSYS2.SYSKEYS K ON ( IDX.INDEX_NAME, IDX.INDEX_SCHEMA ) = ( K.INDEX_NAME, K.INDEX_SCHEMA ) "
               " WHERE TABLE_NAME = ? "
               "   AND TABLE_SCHEMA = ? "
               "ORDER BY IDX.INDEX_NAME, K.ORDINAL_POSITION")
        indexes = cursor.execute(sql, [table_name.upper(), schema.upper()])
        for index_name, column_name in indexes.fetchall():
            if index_name not in constraints:
                constraints[index_name] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            elif constraints[index_name]['unique']:
                continue
            elif constraints[index_name]['primary_key']:
                continue
            constraints[index_name]['columns'].append(column_name.lower())
        return constraints

    def get_sequences(self, cursor, table_name, table_fields=()):
        from django.db import models

        seq_list = []
        for f in table_fields:
            if (isinstance(f, models.AutoField)):
                seq_list.append({'table': table_name, 'column': f.column})
                break
        return seq_list

    def identifier_converter(self, name):
        return name.lower()
