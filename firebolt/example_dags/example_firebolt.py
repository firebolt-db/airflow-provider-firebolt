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
from airflow.providers.firebolt.operators.firebolt import FireboltOperator

FIREBOLT_CONN_ID = 'firebolt_conn_id'
FIREBOLT_SAMPLE_TABLE = 'order_details'
FIREBOLT_DATABASE = 'Sigmoid_Alchemy'
FIREBOLT_ENGINE = 'Sigmoid_Alchemy_Ingest'

# SQL commands
SELECT_STATEMENT_SQL_STRING = f"SELECT * FROM {FIREBOLT_SAMPLE_TABLE} LIMIT 1;"
SQL_INSERT_STATEMENT = (
    f"INSERT INTO {FIREBOLT_SAMPLE_TABLE} values (92,'Oil - Shortening - All - Purpose',"
    f"6928105225,5,4784.12,'2019-06-05','2019-06-05 04:02:08',1); "
)
SQL_LIST = [f"SELECT * FROM {FIREBOLT_SAMPLE_TABLE} LIMIT 2;", "select * from lineitem limit 1;"]
SQL_CREATE_DATABASE_STATEMENT = "CREATE DATABASE IF NOT EXISTS my_db1;"
SQL_DROP_DATABASE_STATEMENT = "DROP DATABASE IF EXISTS my_db1;"
SQL_CREATE_TABLE_STATEMENT = (
    "CREATE FACT TABLE IF NOT EXISTS users12 (id INT, name String, last_login DateTime, password String)"
    " PRIMARY INDEX id;"
)
SQL_DROP_TABLE_STATEMENT = "DROP TABLE IF EXISTS users12;"

dag = DAG(
    'example_firebolt',
    start_date=datetime(2021, 1, 1),
    default_args={'firebolt_conn_id': FIREBOLT_CONN_ID},
    tags=['example'],
    catchup=False,
)


firebolt_op_sql_str = FireboltOperator(
    task_id='firebolt_op_sql_str',
    dag=dag,
    sql=SELECT_STATEMENT_SQL_STRING,
)

firebolt_op_with_params = FireboltOperator(
    task_id='firebolt_op_with_params',
    dag=dag,
    sql=SQL_INSERT_STATEMENT,
    parameters={"id": 56},
)

firebolt_op_sql_list = FireboltOperator(
    task_id='firebolt_op_sql_list',
    dag=dag,
    sql=SQL_LIST,
)

firebolt_op_sql_create_db = FireboltOperator(
    task_id='firebolt_op_sql_create_db',
    dag=dag,
    sql=SQL_CREATE_DATABASE_STATEMENT,
)

firebolt_op_sql_drop_db = FireboltOperator(
    task_id='firebolt_op_sql_drop_db',
    dag=dag,
    sql=SQL_DROP_DATABASE_STATEMENT,
)

firebolt_op_sql_create_table = FireboltOperator(
    task_id='firebolt_op_sql_create_table',
    dag=dag,
    sql=SQL_CREATE_TABLE_STATEMENT,
)

firebolt_op_sql_drop_table = FireboltOperator(
    task_id='firebolt_op_sql_drop_table',
    dag=dag,
    sql=SQL_DROP_TABLE_STATEMENT,
)

(
    firebolt_op_sql_str
    >> [
        firebolt_op_with_params,
        firebolt_op_sql_list,
        firebolt_op_sql_create_db,
        firebolt_op_sql_drop_db,
        firebolt_op_sql_create_table,
        firebolt_op_sql_drop_table,
    ]
)