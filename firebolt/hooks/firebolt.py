#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from contextlib import closing
from typing import Any, Dict, Optional, Union

from firebolt.client import DEFAULT_API_URL
from firebolt.db import Connection, connect
from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from flask_babel import lazy_gettext
from wtforms import StringField

from airflow.hooks.dbapi import DbApiHook


class FireboltHook(DbApiHook):
    """
    A client to interact with Firebolt.

    This hook requires the firebolt_conn_id connection. The firebolt login,
    password, and api_endpoint field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation.

    :param firebolt_conn_id: Reference to
        :ref:`Firebolt connection id<howto/connection:firebolt>`
    :type firebolt_conn_id: str
    :param database: name of firebolt database
    :type database: Optional[str]
    :param engine_name: name of firebolt engine
    :type engine_name: Optional[str]
    """

    conn_name_attr = 'firebolt_conn_id'
    default_conn_name = 'firebolt_default'
    conn_type = 'firebolt'
    hook_name = 'Firebolt'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        return {
            "extra__firebolt__engine__name": StringField(
                lazy_gettext('Engine Name'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra'],
            "relabeling": {'schema': 'Database', 'host': 'API End Point'},
            "placeholders": {
                'host': 'firebolt api end point',
                'schema': 'firebolt database',
                'login': 'firebolt userid',
                'password': 'password',
                'extra__firebolt__engine__name': 'firebolt engine name',
            },
        }

    def __init__(self, *args, **kwargs) -> None:
        """Firebolthook Constructor"""
        super().__init__(*args, **kwargs)
        self.database = kwargs.pop("database", None)
        self.engine_name = kwargs.pop("engine_name", None)

    def _get_conn_params(self) -> Dict[str, Optional[str]]:
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.firebolt_conn_id)  # type: ignore[attr-defined]
        database = conn.schema
        engine_name = conn.extra_dejson.get('extra__firebolt__engine__name', '') or conn.extra_dejson.get(
            'engine_name', ''
        )
        conn_config = {
            "username": conn.login,
            "password": conn.password or '',
            "api_endpoint": conn.host or DEFAULT_API_URL,
            "database": self.database or database,
            "engine_name": self.engine_name or engine_name,
        }
        return conn_config

    def get_conn(self) -> Connection:
        """Return Firebolt connection object"""
        conn_config = self._get_conn_params()
        conn = connect(**conn_config)
        return conn

    def run(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially
        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        """
        scalar = isinstance(sql, str)
        if scalar:
            sql = [sql]
        with closing(self.get_conn()) as conn:
            for query in sql:
                self.log.info(query)
                with closing(conn.cursor()) as cursor:
                    for sql_statement in sql:
                        if parameters:
                            cursor.execute(sql_statement, parameters)
                        else:
                            cursor.execute(sql_statement)
                        execution_info = []
                        if cursor.rowcount > 0:
                            for row in cursor:
                                execution_info.append(row)
                        self.log.info(f"Rows affected: {cursor.rowcount}")
        return execution_info

    def test_connection(self):
        """Test the Firebolt connection by running a simple query."""
        try:
            self.run(sql="select 1")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"
