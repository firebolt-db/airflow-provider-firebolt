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

from firebolt_provider.hooks.firebolt import FireboltHook


class TestFireboltHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.connection = mock.MagicMock()
        self.connection.login = "user"
        self.connection.password = "pw"
        self.connection.schema = "firebolt"

        class UnitTestFireboltHook(FireboltHook):
            conn_name_attr = "firebolt_conn_id"

        self.db_hook = UnitTestFireboltHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch("firebolt_provider.hooks.firebolt.connect")
    def test_get_conn(self, mock_connect):
        self.connection.extra_dejson = {
            "engine_name": "test",
            "account_name": "firebolt",
        }

        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            auth=mock.ANY,
            api_endpoint="api.app.firebolt.io",
            database="firebolt",
            engine_name="test",
            engine_url=None,
            account_name="firebolt",
        )

    @patch("firebolt_provider.hooks.firebolt.connect")
    def test_get_conn_no_extra(self, mock_connect):
        self.connection.extra_dejson = {}

        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            auth=mock.ANY,
            api_endpoint="api.app.firebolt.io",
            database="firebolt",
            engine_name=None,
            engine_url=None,
            account_name=None,
        )

    @patch("firebolt_provider.hooks.firebolt.ResourceManager")
    @patch("firebolt_provider.hooks.firebolt.Settings")
    @patch("firebolt_provider.hooks.firebolt.UsernamePassword")
    def test_get_resource_manager(self, mock_auth, mock_settings, mock_rm):
        self.connection.extra_dejson = {
            "engine_name": "test",
            "account_name": "firebolt",
        }

        self.db_hook.get_resource_manager()

        mock_rm.assert_called_once()
        mock_auth.assert_called_once_with(username="user", password="pw")
        mock_settings.assert_called_once_with(
            auth=mock.ANY,
            server="api.app.firebolt.io",
            default_region="us-east-1",
        )

    @patch("firebolt_provider.hooks.firebolt.ResourceManager")
    @patch("firebolt_provider.hooks.firebolt.Settings")
    @patch("firebolt_provider.hooks.firebolt.UsernamePassword")
    def test_get_resource_manager_custom_api_endpoint(
        self, mock_auth, mock_settings, mock_rm
    ):
        self.connection.extra_dejson = {
            "engine_name": "test",
            "account_name": "firebolt",
            "api_endpoint": "api.dev.firebolt.io",
        }

        self.db_hook.get_resource_manager()

        mock_rm.assert_called_once()
        mock_auth.assert_called_once_with(username="user", password="pw")
        mock_settings.assert_called_once_with(
            auth=mock.ANY,
            server="api.dev.firebolt.io",
            default_region="us-east-1",
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

        mock_engine.stop.assert_called_once_with(wait_for_stop=True)

    @mock.patch(
        "firebolt_provider.hooks.firebolt.FireboltHook.get_resource_manager",
    )
    @mock.patch(
        "firebolt_provider.hooks.firebolt.FireboltHook._get_conn_params",
    )
    @mock.patch(
        "firebolt_provider.hooks.firebolt.get_default_database_engine",
    )
    def test_engine_action_start_default(
        self, default_engine_call, conn_params_call, mock_rm_call
    ):
        mock_rm = MagicMock()
        mock_engine = MagicMock()

        conn_params_call.return_value = {
            "database": "database_name",
            "engine_name": None,
            "engine_url": None,
        }
        mock_rm_call.return_value = mock_rm
        default_engine_call.return_value = mock_engine

        self.db_hook.engine_action(None, "start")
        mock_rm_call.assert_called_once()
        default_engine_call.assert_called_once_with(mock_rm, "database_name")
        conn_params_call.assert_called_once()

        mock_engine.start.assert_called_once_with(wait_for_startup=True)
