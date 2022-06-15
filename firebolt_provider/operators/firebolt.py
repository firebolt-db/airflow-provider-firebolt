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
from typing import Any, List, Optional, Sequence, Union

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.abstractoperator import AbstractOperator
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.context import Context

from firebolt_provider.hooks.firebolt import FireboltHook


def get_db_hook(
    self: Union[
        "FireboltOperator", "FireboltStartEngineOperator", "FireboltStopEngineOperator"
    ]
) -> FireboltHook:
    """
    Create and return FireboltHook.

    :return: a FireboltHook instance.
    :rtype: FireboltHook
    """
    return FireboltHook(
        firebolt_conn_id=self.firebolt_conn_id,
        database=self.database,
        engine_name=self.engine_name,
    )


class RegistryLink(BaseOperatorLink):
    """Link to Registry"""

    name = "Astronomer Registry"

    def get_link(self, operator: AbstractOperator, ti_key: TaskInstanceKey) -> str:
        """Get link to registry page."""

        registry_link = (
            "https://registry.astronomer.io/providers/{provider}/modules/{operator}"
        )
        return registry_link.format(provider="firebolt", operator="fireboltoperator")


class FireboltOperator(BaseOperator):
    """
    Executes SQL code in a Firebolt database

    :param firebolt_conn_id: Firebolt connection id
    :type firebolt_conn_id: str
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param autocommit: if True, each command is automatically committed.
        Currently firebolt doesn't support autocommit feature.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database (will overwrite database defined
        in connection)
    :type database: str
    :param engine_name: name of engine (will overwrite engine_name defined in
        connection)
    :type engine_name: str
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#b4e0ff"

    def __init__(
        self,
        sql: Union[str, List[str]],
        firebolt_conn_id: str = "firebolt_default",
        parameters: Optional[Sequence] = None,
        database: Optional[str] = None,
        engine_name: Optional[str] = None,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.firebolt_conn_id = firebolt_conn_id
        self.sql = sql
        self.database = database
        self.engine_name = engine_name
        self.parameters = parameters
        self.autocommit = autocommit

    def get_db_hook(self) -> FireboltHook:
        return get_db_hook(self)

    def execute(self, context: Context) -> Any:
        """Run query on firebolt"""
        self.log.info("Executing: %s", self.sql)

        hook = self.get_db_hook()
        hook.run(sql=self.sql, autocommit=self.autocommit, parameters=self.parameters)


class FireboltStartEngineOperator(BaseOperator):
    """
    Starts a Firebolt Engine

    :param firebolt_conn_id: Firebolt connection id
    :type firebolt_conn_id: str
    :param engine_name: name of engine, that should be started
    :type engine_name: str
    """

    ui_color = "#f72a30"

    def __init__(
        self,
        engine_name: str,
        firebolt_conn_id: str = "firebolt_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.firebolt_conn_id = firebolt_conn_id
        self.engine_name = engine_name
        self.database = None

    def execute(self, context: Context) -> Any:
        """Starts engine by its name"""
        self.log.info("Start engine: %s", self.engine_name)

        rm = get_db_hook(self).get_resource_manager()
        engine = rm.engines.get_by_name(self.engine_name)

        self.log.info(
            "Found engine: %s, status: %s", engine.name, engine.current_status
        )
        engine.start(wait_for_startup=True)


class FireboltStopEngineOperator(BaseOperator):
    """
    Stops a Firebolt Engine

    :param firebolt_conn_id: Firebolt connection id
    :type firebolt_conn_id: str
    :param engine_name: name of engine, that should be stopped
    :type engine_name: str
    """

    ui_color = "#f72a30"

    def __init__(
        self,
        engine_name: str,
        firebolt_conn_id: str = "firebolt_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.firebolt_conn_id = firebolt_conn_id
        self.engine_name = engine_name
        self.database = None

    def execute(self, context: Context) -> Any:
        """Stops engine by its name"""
        self.log.info("Stop engine: %s", self.engine_name)

        rm = get_db_hook(self).get_resource_manager()
        engine = rm.engines.get_by_name(self.engine_name)

        self.log.info(
            "Found engine: %s, status: %s", engine.name, engine.current_status
        )
        engine.stop(wait_for_stop=True)
