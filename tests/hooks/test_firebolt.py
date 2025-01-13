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
from unittest.mock import MagicMock, patch

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from firebolt.client.auth import ClientCredentials, UsernamePassword
from firebolt.utils.exception import FireboltError, QueryTimeoutError

from firebolt_provider.hooks.firebolt import FireboltHook


class TestFireboltHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.connection = mock.MagicMock()
        self.connection.login = "client_id"
        self.connection.password = "client_secret"
        self.connection.schema = "firebolt"
        self.connection.host = "test"
        self.connection.extra_dejson = {"account_name": "firebolt"}

        class UnitTestFireboltHook(FireboltHook):
            conn_name_attr = "firebolt_conn_id"

        self.db_hook = UnitTestFireboltHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch("firebolt_provider.hooks.firebolt.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            auth=mock.ANY,
            api_endpoint="api.app.firebolt.io",
            database="firebolt",
            engine_name="test",
            account_name="firebolt",
        )
        # Verify that the auth object is a ClientCredentials object
        self.assertIsInstance(mock_connect.call_args[1]["auth"], ClientCredentials)

    @patch("firebolt_provider.hooks.firebolt.connect")
    def test_get_username_pass_conn(self, mock_connect):
        self.connection.login = "usern@me.com"
        self.connection.password = "password"

        self.db_hook.get_conn()

        mock_connect.assert_called_once_with(
            auth=mock.ANY,
            api_endpoint="api.app.firebolt.io",
            database="firebolt",
            engine_name="test",
            account_name="firebolt",
        )
        # Verify that the auth object is a UsernamePassword object
        self.assertIsInstance(mock_connect.call_args[1]["auth"], UsernamePassword)

    @patch("firebolt_provider.hooks.firebolt.connect")
    def test_get_conn_custom_api_endpoint(self, mock_connect):
        self.connection.extra_dejson["api_endpoint"] = "api.mock.firebolt.io"

        self.db_hook.get_conn()

        mock_connect.assert_called_once_with(
            auth=mock.ANY,
            api_endpoint="api.mock.firebolt.io",
            database="firebolt",
            engine_name="test",
            account_name="firebolt",
        )

    @patch("firebolt_provider.hooks.firebolt.ResourceManager")
    @patch("firebolt_provider.hooks.firebolt.ClientCredentials")
    def test_get_resource_manager(self, mock_auth, mock_rm):
        self.connection.extra_dejson["account_name"] = "firebolt"

        self.db_hook.get_resource_manager()

        mock_rm.assert_called_once_with(
            auth=mock.ANY,
            account_name="firebolt",
            api_endpoint="api.app.firebolt.io",
        )
        mock_auth.assert_called_once_with("client_id", "client_secret", True)

    @patch("firebolt_provider.hooks.firebolt.ResourceManager")
    @patch("firebolt_provider.hooks.firebolt.UsernamePassword")
    @patch("firebolt_provider.hooks.firebolt.ClientCredentials")
    def test_get_resource_manager_username_password(
        self, mock_other_auth, mock_auth, mock_rm
    ):
        self.connection.login = "my@username.com"
        self.connection.extra_dejson["account_name"] = "firebolt"

        self.db_hook.get_resource_manager()

        mock_rm.assert_called_once_with(
            auth=mock.ANY,
            account_name="firebolt",
            api_endpoint="api.app.firebolt.io",
        )
        mock_auth.assert_called_once_with("my@username.com", "client_secret", True)
        mock_other_auth.assert_not_called()

    @patch("firebolt_provider.hooks.firebolt.ResourceManager")
    @patch("firebolt_provider.hooks.firebolt.ClientCredentials")
    def test_get_resource_manager_custom_api_endpoint(self, mock_auth, mock_rm):
        self.connection.extra_dejson["api_endpoint"] = "api.dev.firebolt.io"

        self.db_hook.get_resource_manager()

        mock_rm.assert_called_once_with(
            auth=mock.ANY,
            api_endpoint="api.dev.firebolt.io",
            account_name="firebolt",
        )
        mock_auth.assert_called_once_with("client_id", "client_secret", True)


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
        self.conn.cursor().execute.assert_called_once_with(
            sql, parameters, timeout_seconds=None
        )

    def test_run_with_single_query(self):
        sql = "SQL"
        self.db_hook.run(sql)
        self.conn.cursor().execute.assert_called_once_with(sql, timeout_seconds=None)

    def test_run_multi_queries(self):
        sql = ["SQL1", "SQL2"]
        self.db_hook.run(sql, autocommit=True)
        for query in sql:
            self.conn.cursor().execute.assert_any_call(query, timeout_seconds=None)

    def test_get_ui_field_behaviour(self):
        widget = {
            "hidden_fields": ["port", "host"],
            "relabeling": {"schema": "Database"},
            "placeholders": {
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

    @mock.patch(
        "firebolt_provider.hooks.firebolt.FireboltHook.get_resource_manager",
    )
    def test_engine_action_stop(self, mock_rm_call):
        mock_rm = MagicMock()
        mock_engine = MagicMock()

        mock_rm_call.return_value = mock_rm
        mock_rm.engines.get_by_name.return_value = mock_engine

        self.db_hook.engine_action("engine_name", "stop")
        mock_rm_call.assert_called_once()
        mock_rm.engines.get_by_name.assert_called_once_with("engine_name")

        mock_engine.stop.assert_called_once()

    @mock.patch(
        "firebolt_provider.hooks.firebolt.FireboltHook._get_conn_params",
    )
    def test_engine_action_start_default(self, conn_params_call):
        conn_params_call.return_value = FireboltHook.ConnectionParameters(
            client_id=None,
            client_secret=None,
            account_name=None,
            database="database_name",
            engine_name=None,
            api_endpoint=None,
        )

        with self.assertRaises(FireboltError):
            self.db_hook.engine_action(None, "start")

    def test_run_returns_results(self):
        sql = ["SQL1", "SQL2"]
        self.cursor.fetchall.return_value = [(1, 2)]
        res = self.db_hook.run(sql, handler=fetch_all_handler)
        assert res == [[(1, 2)], [(1, 2)]]

        sql = "SQL1; SQL2"
        res = self.db_hook.run(
            sql,
            handler=fetch_all_handler,
            return_last=False,
            split_statements=True,
        )
        assert res == [[(1, 2)], [(1, 2)]]

        res = self.db_hook.run(
            sql,
            handler=fetch_all_handler,
            return_last=True,
            split_statements=True,
        )
        assert res == [(1, 2)]

    def test_timeout(self):
        self.db_hook.query_timeout = 1
        self.cursor.execute.side_effect = QueryTimeoutError("Timeout")
        with self.assertRaises(QueryTimeoutError):
            self.db_hook.run("SELECT 1")

        self.conn.cursor().execute.assert_called_once_with(
            "SELECT 1", timeout_seconds=1
        )
        self.conn.cursor().execute.reset_mock()

        self.db_hook.fail_on_query_timeout = False
        assert self.db_hook.run("SELECT 1") is None

        self.conn.cursor().execute.assert_called_once_with(
            "SELECT 1", timeout_seconds=1
        )
