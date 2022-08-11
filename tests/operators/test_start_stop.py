from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from firebolt_provider.operators.firebolt import (
    FireboltStartEngineOperator,
    FireboltStopEngineOperator,
)


@pytest.mark.parametrize("operator", ["start", "stop"])
def test_start_stop_operator(operator: str, mocker: MockerFixture):
    """test start operator executes engine.start"""
    get_db_hook_mock = mocker.patch("firebolt_provider.operators.firebolt.get_db_hook")

    db_hook_mock = MagicMock()
    get_db_hook_mock.return_value = db_hook_mock

    operator_params = {"task_id": "task_id", "engine_name": "engine_name"}

    if operator == "start":
        engine_operator = FireboltStartEngineOperator(**operator_params)
    else:
        engine_operator = FireboltStopEngineOperator(**operator_params)

    engine_operator.execute({})

    if operator == "start":
        db_hook_mock.engine_action.assert_called_once_with("engine_name", "start")
    else:
        db_hook_mock.engine_action.assert_called_once_with("engine_name", "stop")
