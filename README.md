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


# Package airflow-providers-firebolt

Release: 0.0.1

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Configuration](#configuration)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 0.0.1](#release-001)

## Provider package

This is a provider package for `firebolt` provider. All classes for this provider package
are in `firebolt_provider` python package.



## Installation

You can install this package on top of an existing airflow 2.* installation via

`pip install airflow-providers-firebolt`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| firebolt-sdk  | &gt;=0.2.0         |

## Configuration

In the Airflow user interface, configure a Connection for Firebolt. Most of the Connection config fields will be left blank. Configure the following fields:

* `Conn Id`: `firebolt_conn_id`
* `Conn Type`: `Firebolt`
* `Login`: Firebolt Login
* `Password`: Firebolt Password
* `Engine_Name`: Firebolt Engine Name

# Provider classes summary

In Airflow 2.0, all operators, hooks for the `firebolt` provider
are in the `firebolt_provider` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `firebolt_provider` package                                                                 |
|:------------------------------------------------------------------------------------------------------------------------------|
| [operators.firebolt.FireboltOperator](https://github.com/firebolt-db/airflow-provider-firebolt/blob/main/firebolt_provider/operators/firebolt.py) |




## Hooks


### New hooks

| New Airflow 2.0 hooks: `firebolt_provider` package                                                         |
|:------------------------------------------------------------------------------------------------------------------|
| [hooks.firebolt.FireboltHook](https://github.com/firebolt-db/airflow-provider-firebolt/blob/main/firebolt_provider/hooks/firebolt.py) |


## Releases

### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| |
