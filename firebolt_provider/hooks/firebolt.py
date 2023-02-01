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

import json
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from airflow.version import version as airflow_version
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import UsernamePassword
from firebolt.common import Settings
from firebolt.db import Connection, connect
from firebolt.model.engine import Engine
from firebolt.service.manager import ResourceManager
from firebolt.utils.exception import FireboltError

if airflow_version.startswith("1.10"):
    from airflow.hooks.dbapi_hook import DbApiHook  # type: ignore
    from airflow.models.connection import Connection as AirflowConnection

    # Show Firebolt in list of connections in UI
    if ("firebolt", "Firebolt") not in AirflowConnection._types:
        AirflowConnection._types.append(("firebolt", "Firebolt"))
else:
    # Airflow 2.0 path for the base class
    from airflow.hooks.dbapi import DbApiHook


def get_default_database_engine(rm: ResourceManager, database_name: str) -> Engine:
    """
    Get the default engine of the database. If the default engine doesn't exists
    raise FireboltError
    """

    database = rm.databases.get_by_name(name=database_name)
    bindings = rm.bindings.get_many(database_id=database.database_id)

    if len(bindings) == 0:
        raise FireboltError("No engines attached to the database")

    for binding in bindings:
        if binding.is_default_engine:
            return rm.engines.get(binding.engine_id)

    raise FireboltError("No default engine is found.")


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

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port", "host"],
            "relabeling": {
                "schema": "Database",
                "login": "Username",
                "extra": "Advanced Connection Properties",
            },
            "placeholders": {
                "schema": "The name of the Firebolt database to connect to",
                "login": "The email address you use to log in to Firebolt",
                "password": "The password you use to log in to Firebolt",
                "extra": json.dumps(
                    {
                        "account_name": "The Firebolt account to log in to",
                        "engine_name": "The Firebolt engine name or URL to run SQL on",
                    },
                ),
            },
        }

    def __init__(self, *args: Optional[str], **kwargs: Optional[str]) -> None:
        """Firebolthook Constructor"""
        super().__init__(*args, **kwargs)
        self.database = kwargs.pop("database", None)
        self.engine_name = kwargs.pop("engine_name", None)

    def _get_conn_params(self) -> Dict[str, Optional[str]]:
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn_id = getattr(self, self.conn_name_attr)
        conn = self.get_connection(conn_id)
        database = conn.schema

        engine_name = self.engine_name or conn.extra_dejson.get("engine_name", None)

        engine_url = None
        if engine_name and "." in engine_name:
            engine_name, engine_url = None, engine_name

        api_endpoint = conn.extra_dejson.get("api_endpoint", None)
        account_name = conn.extra_dejson.get("account_name", None)
        conn_config = {
            "username": conn.login,
            "password": conn.password or "",
            "api_endpoint": api_endpoint or DEFAULT_API_URL,
            "database": self.database or database,
            "engine_name": engine_name,
            "engine_url": engine_url,
            "account_name": account_name,
        }
        return conn_config

    def get_conn(self) -> Connection:
        """Return Firebolt connection object"""
        conn_config = self._get_conn_params()
        username, password = conn_config["username"], conn_config["password"]
        if not (username and password):
            raise FireboltError("Either username or password is missing")

        conn = connect(
            auth=UsernamePassword(username=username, password=password),
            api_endpoint=conn_config["api_endpoint"],
            database=conn_config["database"],
            engine_name=conn_config["engine_name"],
            engine_url=conn_config["engine_url"],
            account_name=conn_config["account_name"],
        )

        return conn

    def get_resource_manager(self) -> ResourceManager:
        """Return Resource Manager"""
        conn_config = self._get_conn_params()
        username, password = conn_config["username"], conn_config["password"]
        if not (username and password):
            raise FireboltError("Either username or password is missing")

        manager = ResourceManager(
            Settings(
                auth=UsernamePassword(username=username, password=password),
                server=conn_config["api_endpoint"],
                default_region="us-east-1",
            )
        )

        return manager

    def engine_action(self, engine_name: Optional[str], action: str) -> None:
        """
        Performs start or stop of the engine

        Args:
            engine_name: name of the engine, if None, the engine from
             the connection will be used
            action: either stop or start

        """
        if engine_name is None:
            self.log.info(
                "engine_name is not set, getting engine_name from connection config"
            )
            conn_config = self._get_conn_params()
            database_name = conn_config.get("database")

            engine_name = conn_config.get("engine_name", None)
            engine_url = conn_config.get("engine_url", None)
            if engine_url:
                raise FireboltError("Cannot start/stop engine using its URL")

        rm = self.get_resource_manager()
        if engine_name is None:
            if database_name is None:
                raise FireboltError("Database must be set")
            engine = get_default_database_engine(rm, database_name)
        else:
            engine = rm.engines.get_by_name(engine_name)

        if action == "start":
            engine.start(wait_for_startup=True)
        elif action == "stop":
            engine.stop(wait_for_stop=True)
        else:
            raise FireboltError(f"unknown action {action}")

    def run(
        self,
        sql: Union[str, List],
        autocommit: bool = False,
        parameters: Optional[Sequence] = None,
        handler: Optional[Callable] = None,
    ) -> None:
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
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                for sql_statement in sql:
                    if parameters:
                        cursor.execute(sql_statement, parameters)
                    else:
                        cursor.execute(sql_statement)
                    self.log.info(f"Rows returned: {cursor.rowcount}")

    def test_connection(self) -> Tuple[bool, str]:
        """Test the Firebolt connection by running a simple query."""
        try:
            self.run(sql="select 1")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"
