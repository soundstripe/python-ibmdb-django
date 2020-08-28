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

        schema = None
        if table_type == 'A':  # alias
            sql = """SELECT BASENAME, BASESCHEMA
                       FROM TABLE( SYSPROC.BASE_TABLE( CURRENT_SCHEMA, ? ) ) AS X"""
            cursor.execute(sql, [table_name.upper()])
            table_name, schema = cursor.fetchone()

        if table_type != 'X':
            sql = """SELECT TRIM( COLUMN_NAME ) AS COLUMN_NAME
                          , TRIM( DATA_TYPE )   AS DATA_TYPE
                          , LENGTH              AS DISPLAY_SIZE
                          , STORAGE             AS INTERNAL_SIZE
                          , NUMERIC_PRECISION
                          , NUMERIC_SCALE
                          , CASE IS_NULLABLE WHEN 'Y' THEN 1 ELSE 0 END AS IS_NULLABLE
                     FROM QSYS2.SYSCOLUMNS C
                     WHERE TABLE_NAME = ?
                       AND TABLE_SCHEMA = COALESCE(?, CURRENT_SCHEMA)
                     ORDER BY ORDINAL_POSITION"""
            column_descriptions = cursor.execute(sql, [table_name.upper(), schema])

            for desc in column_descriptions:
                description.append(FieldInfo(
                    self.identifier_converter(desc[0]),
                    desc[1],
                    *desc[2:],
                    None
                ))
        return description

    def get_constraints(self, cursor, table_name):
        constraints = {}
        schema = None
        sql = "SELECT TYPE FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA=CURRENT_SCHEMA AND TABLE_NAME=?"
        cursor.execute(sql, [table_name.upper()])
        table_type = cursor.fetchone()[0]

        schema = None
        if table_type == 'A':  # alias
            sql = """SELECT BASENAME, BASESCHEMA
                       FROM TABLE( SYSPROC.BASE_TABLE( CURRENT_SCHEMA, ? ) ) AS X"""
            cursor.execute(sql, [table_name.upper()])
            table_name, schema = cursor.fetchone()

        sql = """SELECT CST.CONSTRAINT_NAME
                     , COLUMN_NAME
                     , CASE CONSTRAINT_TYPE WHEN 'PRIMARY KEY' THEN 1 ELSE 0 END AS IS_PRIMARY
                     , CASE CONSTRAINT_TYPE WHEN 'UNIQUE' THEN 1 ELSE 0 END      AS IS_UNIQUE
                     , CASE CONSTRAINT_TYPE WHEN 'FOREIGN KEY' THEN 1 ELSE 0 END AS IS_FOREIGN
                     , CASE CONSTRAINT_TYPE WHEN 'CHECK' THEN 1 ELSE 0 END       AS IS_CHECK
                FROM QSYS2.SYSCST CST
                         JOIN QSYS2.SYSCSTCOL CSTCOL
                              ON ( CST.CONSTRAINT_NAME, CST.CONSTRAINT_SCHEMA ) = ( CSTCOL.CONSTRAINT_NAME, CSTCOL.CONSTRAINT_SCHEMA)
                WHERE CST.TABLE_NAME = ? and CST.TABLE_SCHEMA = CURRENT_SCHEMA"""
        cursor.execute(sql, [table_name.upper()])
        for constraint_name, column_name, is_primary, is_unique, is_foreign, is_check in cursor.fetchall():
            constraints.setdefault(self.identifier_converter(constraint_name), {
                'columns': [],
                'primary_key': is_primary,
                'unique': is_unique,
                'foreign_key': is_foreign,
                'check': is_check,
                'index': False
            })['columns'].append(self.identifier_converter(column_name))

        sql = ("SELECT IDX.INDEX_NAME, K.COLUMN_NAME, CASE IDX.IS_UNIQUE WHEN 'D' THEN 0 ELSE 1 END AS IS_UNIQUE "
               "  FROM QSYS2.SYSINDEXES IDX "
               "  JOIN QSYS2.SYSKEYS K ON ( IDX.INDEX_NAME, IDX.INDEX_SCHEMA ) = ( K.INDEX_NAME, K.INDEX_SCHEMA ) "
               " WHERE TABLE_NAME = ? "
               "   AND TABLE_SCHEMA = CURRENT_SCHEMA "
               "ORDER BY IDX.INDEX_NAME, K.ORDINAL_POSITION")
        indexes = cursor.execute(sql, [table_name.upper()])
        for index_name, column_name, is_unique in indexes.fetchall():
            constraints.setdefault(self.identifier_converter(index_name), {
                'columns': [],
                'primary_key': False,
                'unique': is_unique,
                'foreign_key': None,
                'check': False,
                'index': True,
            })['columns'].append(self.identifier_converter(column_name))
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
