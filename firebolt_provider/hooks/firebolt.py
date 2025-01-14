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

from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple, Union

from airflow.version import version as airflow_version
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import Auth, ClientCredentials, UsernamePassword
from firebolt.db import Connection, Cursor, connect
from firebolt.model.V1.engine import Engine as EngineV1
from firebolt.model.V2.engine import Engine as EngineV2
from firebolt.service.manager import ResourceManager
from firebolt.utils.exception import FireboltError, QueryTimeoutError

if airflow_version.startswith("1.10"):
    from airflow.hooks.dbapi_hook import DbApiHook  # type: ignore
    from airflow.models.connection import Connection as AirflowConnection

    # Show Firebolt in list of connections in UI
    if ("firebolt", "Firebolt") not in AirflowConnection._types:
        AirflowConnection._types.append(("firebolt", "Firebolt"))
else:
    # Airflow 2.0 path for the base class
    from airflow.hooks.dbapi import DbApiHook


class FireboltHook(DbApiHook):
    """
    A client to interact with Firebolt.
    This hook requires the firebolt_conn_id connection. The firebolt login,
    password, and api_endpoint field must be setup in the connection.
    Other inputs can be defined in the connection or hook instantiation.

    :param firebolt_conn_id: Reference to
        :ref:`Firebolt connection id<howto/connection:firebolt>`
    :type firebolt_conn_id: str
    :param database: name of firebolt database
    :type database: Optional[str]
    :param engine_name: name of firebolt engine
    :type engine_name: Optional[str]
    """

    conn_name_attr = "firebolt_conn_id"
    default_conn_name = "firebolt_default"
    conn_type = "firebolt"
    hook_name = "Firebolt"
    supports_autocommit = False

    ConnectionParameters = namedtuple(
        "ConnectionParameters",
        [
            "client_id",
            "client_secret",
            "api_endpoint",
            "database",
            "engine_name",
            "account_name",
        ],
    )

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        from flask_appbuilder.fieldwidgets import (  # type: ignore
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext  # type: ignore
        from wtforms import StringField  # type: ignore

        return {
            "account_name": StringField(
                lazy_gettext("Account"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port"],
            "relabeling": {
                "schema": "Database",
                "login": "Client ID",
                "password": "Client Secret",
                # Store engine name in host for better navigation
                # on a connection list page
                "host": "Engine",
            },
            "placeholders": {
                "schema": "The name of the Firebolt database to connect to",
                "login": "The client id of your service account",
                "password": "The client secret of your service account",
                "account_name": "The Firebolt account to log in to",
                "host": "The Firebolt engine name to run SQL on",
            },
        }

    def __init__(
        self,
        database: Optional[str] = None,
        engine_name: Optional[str] = None,
        query_timeout: Optional[float] = None,
        fail_on_query_timeout: bool = True,
        *args: Optional[str],
        **kwargs: Optional[str],
    ) -> None:
        """Firebolthook Constructor"""
        super().__init__(*args, **kwargs)
        self.database = database
        self.engine_name = engine_name
        self.query_timeout = query_timeout
        self.fail_on_query_timeout = fail_on_query_timeout

    def _get_conn_params(self) -> "ConnectionParameters":
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn_id = getattr(self, self.conn_name_attr)
        conn = self.get_connection(conn_id)
        database = self.database or conn.schema

        engine_name = self.engine_name or conn.host
        api_endpoint = conn.extra_dejson.get("api_endpoint", DEFAULT_API_URL)
        account_name = conn.extra_dejson.get("account_name", None)

        if not (conn.login and conn.password):
            raise FireboltError("Authentication credentials are missing")

        return self.ConnectionParameters(
            client_id=conn.login,
            client_secret=conn.password,
            api_endpoint=api_endpoint,
            database=database,
            engine_name=engine_name,
            account_name=account_name,
        )

    def get_conn(self) -> Connection:
        """Return Firebolt connection object"""
        conn_config = self._get_conn_params()
        auth = _determine_auth(conn_config.client_id, conn_config.client_secret)
        conn = connect(
            auth=auth,
            api_endpoint=conn_config.api_endpoint,
            database=conn_config.database,
            engine_name=conn_config.engine_name,
            account_name=conn_config.account_name,
        )

        return conn

    def get_resource_manager(self) -> ResourceManager:
        """Return Resource Manager"""
        conn_config = self._get_conn_params()
        auth = _determine_auth(conn_config.client_id, conn_config.client_secret)

        manager = ResourceManager(
            auth=auth,
            api_endpoint=conn_config.api_endpoint,
            account_name=conn_config.account_name,
        )

        return manager

    def _run_command(
        self, cur: Cursor, sql_statement: str, parameters: Optional[List[str]]
    ) -> None:
        """Run a statement using an already open cursor."""
        if self.log_sql:
            self.log.info(
                "Running statement: %s, parameters: %s", sql_statement, parameters
            )

        if parameters:
            cur.execute(sql_statement, parameters, timeout_seconds=self.query_timeout)
        else:
            cur.execute(sql_statement, timeout_seconds=self.query_timeout)

        # According to PEP 249, this is -1 when query result is not applicable.
        if cur.rowcount >= 0:
            self.log.info("Rows affected: %s", cur.rowcount)

    def run(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return super().run(*args, **kwargs)
        except QueryTimeoutError:
            if self.fail_on_query_timeout:
                raise
            return None

    def _run_action(self, engine: Union[EngineV1, EngineV2], action: str) -> None:
        if action == "start":
            engine.start()
        elif action == "stop":
            engine.stop()
        else:
            raise FireboltError(f"unknown action {action}")

    def _get_engine(self, engine_name: Optional[str]) -> Union[EngineV1, EngineV2]:
        if engine_name is None:
            self.log.info(
                "engine_name is not set, getting engine_name from connection config"
            )
            conn_config = self._get_conn_params()
            engine_name = conn_config.engine_name
            if engine_name is None:
                raise FireboltError("Engine name must be provided")

        rm = self.get_resource_manager()
        return rm.engines.get_by_name(engine_name)

    def engine_action(self, engine_name: Optional[str], action: str) -> None:
        """
        Performs start or stop of the engine

        Args:
            engine_name: name of the engine, if None, the engine from
             the connection will be used
            action: either stop or start

        """
        self._run_action(self._get_engine(engine_name), action)

    def test_connection(self) -> Tuple[bool, str]:
        """Test the Firebolt connection by running a simple query."""
        try:
            self.run(sql="select 1")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"


def _determine_auth(key: str, secret: str, token_cache_flag: bool = True) -> Auth:
    if "@" in key:
        return UsernamePassword(key, secret, token_cache_flag)
    else:
        return ClientCredentials(key, secret, token_cache_flag)
