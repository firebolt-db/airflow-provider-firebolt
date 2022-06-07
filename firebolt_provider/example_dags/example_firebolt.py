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
"""
Example use of Firebolt related operators.
"""
from datetime import datetime

from airflow import DAG

from firebolt_provider.operators.firebolt import (
    FireboltOperator,
    FireboltStartEngineOperator,
    FireboltStopEngineOperator,
)

FIREBOLT_CONN_ID = "firebolt_conn_id"
FIREBOLT_SAMPLE_TABLE = "sample_table"
FIREBOLT_DATABASE = "sample_database"
FIREBOLT_ENGINE = "sample_engine"

# SQL commands
SQL_CREATE_TABLE_STATEMENT = (
    f"CREATE FACT TABLE IF NOT EXISTS {FIREBOLT_SAMPLE_TABLE} "
    f"(id INT, name String)"
    f" PRIMARY INDEX id;"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {FIREBOLT_SAMPLE_TABLE} values (%(id)s, 'name');"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SELECT_STATEMENT_SQL_STRING = f"SELECT * FROM {FIREBOLT_SAMPLE_TABLE} LIMIT 10;"
SQL_DROP_TABLE_STATEMENT = f"DROP TABLE IF EXISTS {FIREBOLT_SAMPLE_TABLE};"

SQL_CREATE_DATABASE_STATEMENT = f"CREATE DATABASE IF NOT EXISTS {FIREBOLT_DATABASE};"
SQL_DROP_DATABASE_STATEMENT = f"DROP DATABASE IF EXISTS {FIREBOLT_DATABASE};"

with DAG(
    "example_firebolt",
    start_date=datetime(2021, 1, 1),
    default_args={"firebolt_conn_id": FIREBOLT_CONN_ID},
    tags=["example"],
    catchup=False,
) as dag:
    firebolt_start_engine = FireboltStartEngineOperator(
        task_id="firebolt_start_engine", engine_name=FIREBOLT_ENGINE
    )
    firebolt_stop_engine = FireboltStopEngineOperator(
        task_id="firebolt_stop_engine", engine_name=FIREBOLT_ENGINE
    )

    firebolt_op_sql_create_table = FireboltOperator(
        task_id="firebolt_op_sql_create_table",
        sql=SQL_CREATE_TABLE_STATEMENT,
    )

    firebolt_op_sql_list = FireboltOperator(
        task_id="firebolt_op_sql_list",
        sql=SQL_LIST,
    )

    firebolt_op_with_params = FireboltOperator(
        task_id="firebolt_op_with_params",
        sql=SQL_INSERT_STATEMENT,
        parameters=[56],
    )

    firebolt_op_sql_str = FireboltOperator(
        task_id="firebolt_op_sql_str",
        sql=SELECT_STATEMENT_SQL_STRING,
    )

    firebolt_op_sql_drop_table = FireboltOperator(
        task_id="firebolt_op_sql_drop_table",
        sql=SQL_DROP_TABLE_STATEMENT,
    )

    firebolt_op_sql_create_db = FireboltOperator(
        task_id="firebolt_op_sql_create_db",
        sql=SQL_CREATE_DATABASE_STATEMENT,
    )

    firebolt_op_sql_drop_db = FireboltOperator(
        task_id="firebolt_op_sql_drop_db",
        sql=SQL_DROP_DATABASE_STATEMENT,
    )

    (
        firebolt_start_engine
        >> firebolt_op_sql_create_table
        >> firebolt_op_sql_list
        >> firebolt_op_with_params
        >> firebolt_op_sql_str
        >> firebolt_op_sql_drop_table
        >> firebolt_op_sql_create_db
        >> firebolt_op_sql_drop_db
        >> firebolt_stop_engine
    )
