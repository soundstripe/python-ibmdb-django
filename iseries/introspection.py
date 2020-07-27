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
        'VARCHAR': 'CharField',
        'CHAR': 'CharField',
        'DATE': 'DateField',
        'TIMESTAMP': 'DateTimeField',
        'TIMESTMP': 'DateTimeField',
        'DECIMAL': 'DecimalField',
        'DOUBLE': 'FloatField',
        'INTEGER': 'IntegerField',
        'BIGINT': 'BigIntegerField',
        'SMALLINT': 'SmallIntegerField',
        'CLOB': 'TextField',
        'TIME': 'TimeField',
        'XML': 'XMLField',
        'BLOB': 'BinaryField',
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
        foreign_keys = list(cursor.foreignKeys(foreignTable=table_name.upper(), schema=schema))
        for fk in foreign_keys:
            relations[self.identifier_converter(fk.fkcolumn_name)] = (
                self.identifier_converter(fk.pktable_name),
                self.identifier_converter(fk.pkcolumn_name),
            )
        return relations

    # Private method. Getting Index position of column by its name
    def __get_col_index(self, cursor, schema, table_name, col_name):
        cols = cursor.columns(schema=schema, table=table_name, column=col_name)
        for col in cols:
            return col.ordinal_position - 1

    def get_key_columns(self, cursor, table_name):
        relations = []
        schema = cursor.get_current_schema()
        foreign_keys = list(cursor.foreignKeys(table=table_name.upper(), schema=schema))
        for fk in foreign_keys:
            relations.append((self.identifier_converter(fk.fkcolumn_name),
                              self.identifier_converter(fk.pktable_name),
                              self.identifier_converter(fk.pkcolumn_name)))
        return relations

    # Getting the description of the table.
    def get_table_description(self, cursor, table_name):
        qn = self.connection.ops.quote_name
        description = []

        sql = "SELECT TYPE FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA=CURRENT_SCHEMA AND TABLE_NAME=?"
        cursor.execute(sql, [table_name.upper()])
        table_type = cursor.fetchone()[0]

        if table_type != 'X':
            sql = "SELECT TRIM(column_name), TRIM(data_type) " \
                  "  FROM QSYS2.SYSCOLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = CURRENT_SCHEMA"
            column_data_types = cursor.execute(sql, [table_name.upper()])
            column_data_types = dict(column_data_types.fetchall())

            cursor.execute("SELECT * FROM %s FETCH FIRST 1 ROWS ONLY" % qn(table_name))
            for desc in cursor.description:
                description.append(FieldInfo(
                    self.identifier_converter(desc[0]),
                    column_data_types[desc[0]],
                    *desc[2:],
                    None
                ))

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
            constraints[constname]['columns'].append(self.identifier_converter(colname))

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
            constraints[constname]['columns'].append(self.identifier_converter(colname))

        for pkey in cursor.primaryKeys(schema=schema, table=table_name):
            if pkey.pk_name not in constraints:
                constraints[pkey.pk_name] = {
                    'columns': [],
                    'primary_key': True,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[pkey.pk_name]['columns'].append(self.identifier_converter(pkey.column_name))

        for fk in cursor.foreignKeys(table=table_name.upper(), schema=schema):
            if fk.fk_name not in constraints:
                constraints[fk.fk_name] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (self.identifier_converter(fk.pktable_name), self.identifier_converter(fk.pkcolumn_name)),
                    'check': False,
                    'index': False
                }
            constraints[fk.fk_name]['columns'].append(self.identifier_converter(fk.fkcolumn_name))
            if self.identifier_converter(fk.pkcolumn_name) not in constraints[fk.fk_name]['foreign_key']:
                fkeylist = list(constraints[fk.fk_name]['foreign_key'])
                fkeylist.append(self.identifier_converter(fk.pkcolumn_name))
                constraints[fk.fk_name]['foreign_key'] = tuple(fkeylist)

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
            constraints[index_name]['columns'].append(self.identifier_converter(column_name))
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
