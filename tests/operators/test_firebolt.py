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

import unittest
from unittest import mock

import pytest

from firebolt_provider.operators.firebolt import FireboltOperator


class TestFireboltOperator(unittest.TestCase):
    @mock.patch("firebolt_provider.operators.firebolt.FireboltHook")
    def test_execute(self, mock_hook):
        sql = "SELECT 1"
        autocommit = True
        parameters = {"value": 1}
        operator = FireboltOperator(
            task_id="test_task_id",
            sql=sql,
            autocommit=autocommit,
            parameters=parameters,
        )
        operator.execute({})
        mock_hook.return_value.run.assert_called_once_with(
            sql=sql, autocommit=autocommit, parameters=parameters
        )


class TestGetDBHook:
    @mock.patch("firebolt_provider.operators.firebolt.FireboltHook")
    @pytest.mark.parametrize(
        "kwargs, expected_kwargs",
        [
            (
                {"sql": "Select * from test_table"},
                {
                    "query_timeout": None,
                    "fail_on_query_timeout": True,
                    "database": None,
                    "engine_name": None,
                    "firebolt_conn_id": "firebolt_default",
                },
            ),
            (
                {
                    "sql": "Select * from test_table",
                    "query_timeout": 10,
                    "fail_on_query_timeout": False,
                },
                {
                    "query_timeout": 10,
                    "fail_on_query_timeout": False,
                    "database": None,
                    "engine_name": None,
                    "firebolt_conn_id": "firebolt_default",
                },
            ),
        ],
    )
    def test_get_db_hook(
        self,
        mock_get_db_hook,
        kwargs,
        expected_kwargs,
    ):
        operator = FireboltOperator(
            task_id="test_task_id", firebolt_conn_id="firebolt_default", **kwargs
        )
        operator.get_db_hook()
        mock_get_db_hook.assert_called_once_with(**expected_kwargs)
