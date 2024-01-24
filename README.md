<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Firebolt Provider for Apache Airflow

[![Unit tests](https://github.com/firebolt-db/airflow-provider-firebolt/actions/workflows/pull-request.yml/badge.svg)](https://github.com/firebolt-db/airflow-provider-firebolt/actions/workflows/pull-request.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/yuryfirebolt/22e274394fed6421b6f5d5a2c8016fa3/raw/firebolt-airflow-provider-coverage.json
)


<img width="1114" alt="Screen Shot 2022-02-02 at 2 57 37 PM" src="https://user-images.githubusercontent.com/7674553/152251803-427f45b5-2160-4434-9f3e-431db4d3e79e.png">

This is the provider package for the `firebolt` provider. All classes for this provider package are in the `firebolt_provider` Python package.

## Contents

- <a href="#installation">Installation</a>[]()
- <a href="#configuration">Configuration</a>[]()
- <a href="#modules">Modules</a>[]()
    - <a href="#operators">Operators</a>[]()
    - <a href="#hooks">Hooks</a>[]()


<a id="installation"></a>
## Installation

You can install this package via

```shell
pip install airflow-provider-firebolt
```

`airflow-provider-firebolt` requires `apache-airflow` 2.0+ and `firebolt-sdk` 1.1+.


<a id="configuration"></a>
## Configuration

In the Airflow user interface, configure a Connection for Firebolt. Configure the following fields:

* `Conn Id`: `firebolt_conn_id`.
* `Conn Type`: `Firebolt`.
* `Client ID`: Service account ID.
* `Client Secret`: Service account secret.
* `Engine_Name`: Firebolt Engine Name.
* `Account`: Name of the account you're connecting to.

Client id and secret credentials can be obtained by registering a [Service account](https://docs.firebolt.io/godocs/Guides/managing-your-organization/service-accounts.html#manage-service-accounts).

### Note
If you're accessing Firebolt UI via `app.firebolt.io` then use Username and Password instead of Client ID and Client Secret to connect.

<a id="modules"></a>
## Modules


<a id="operators"></a>
### Operators

[operators.firebolt.FireboltOperator](https://github.com/firebolt-db/airflow-provider-firebolt/blob/main/firebolt_provider/operators/firebolt.py) runs a provided SQL script against Firebolt and returns results.

[operators.firebolt.FireboltStartEngineOperator](https://github.com/firebolt-db/airflow-provider-firebolt/blob/main/firebolt_provider/operators/firebolt.py)
[operators.firebolt.FireboltStopEngineOperator](https://github.com/firebolt-db/airflow-provider-firebolt/blob/main/firebolt_provider/operators/firebolt.py) starts/stops the specified engine, and waits until it is actually started/stopped. If the `engine_name` is not specified, it will use the `engine_name` from the connection, if it also not specified it will start the default engine of the connection database. Note: start/stop operator requires actual engine name, if engine URL is specified instead, start/stop engine operators will not be able to handle it correctly.




<a id="hooks"></a>
### Hooks

[hooks.firebolt.FireboltHook](https://github.com/firebolt-db/airflow-provider-firebolt/blob/main/firebolt_provider/hooks/firebolt.py) establishes a connection to Firebolt.

## Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/airflow-provider-firebolt/tree/main/CONTRIBUTING.MD)
