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


import json
import unittest
from unittest import mock
from unittest.mock import patch

from firebolt_provider.hooks.firebolt import FireboltHook


class TestFireboltHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.connection = mock.MagicMock()
        self.connection.login = "user"
        self.connection.password = "pw"
        self.connection.schema = "firebolt"
        self.connection.host = "api_endpoint"
        self.connection.extra_dejson = {"engine_name": "test"}

        class UnitTestFireboltHook(FireboltHook):
            conn_name_attr = "firebolt_conn_id"

        self.db_hook = UnitTestFireboltHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch("firebolt_provider.hooks.firebolt.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            username="user",
            password="pw",
            api_endpoint="api_endpoint",
            database="firebolt",
            engine_name="test",
        )


class TestFireboltHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.conn = mock.MagicMock()
        self.cursor = mock.MagicMock(rowcount=0)
        self.conn.cursor.return_value = self.cursor
        conn = self.conn

        class UnitTestFireboltHook(FireboltHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestFireboltHook()

    def test_run_with_parameters(self):
        sql = "SQL"
        parameters = ("param1", "param2")
        self.db_hook.run(sql=sql, parameters=parameters)
        self.conn.__enter__().cursor().__enter__().execute.assert_called_once_with(
            sql, parameters
        )

    def test_run_with_single_query(self):
        sql = "SQL"
        self.db_hook.run(sql)
        self.conn.__enter__().cursor().__enter__().execute.assert_called_once_with(sql)

    def test_run_multi_queries(self):
        sql = ["SQL1", "SQL2"]
        self.db_hook.run(sql, autocommit=True)
        for query in sql:
            self.conn.__enter__().cursor().__enter__().execute.assert_any_call(query)

    def test_get_ui_field_behaviour(self):
        widget = {
            "hidden_fields": ["port"],
            "relabeling": {"schema": "Database", "host": "API End Point"},
            "placeholders": {
                "host": "firebolt api end point",
                "schema": "firebolt database",
                "login": "firebolt userid",
                "password": "password",
                "extra": json.dumps(
                    {
                        "engine_name": "firebolt engine name",
                    },
                ),
            },
        }
        self.db_hook.get_ui_field_behaviour() == widget

    @mock.patch("firebolt_provider.hooks.firebolt.FireboltHook.run")
    def test_connection_success(self, mock_run):
        mock_run.return_value = [{"1": 1}]
        status, msg = self.db_hook.test_connection()
        assert status is True
        assert msg == "Connection successfully tested"

    @mock.patch(
        "firebolt_provider.hooks.firebolt.FireboltHook.run",
        side_effect=Exception("Connection Errors"),
    )
    def test_connection_failure(self, mock_run):
        mock_run.return_value = [{"1": 1}]
        status, msg = self.db_hook.test_connection()
        assert status is False
        assert msg == "Connection Errors"
