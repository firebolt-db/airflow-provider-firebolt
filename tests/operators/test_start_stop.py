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
    rm_mock = MagicMock()
    engine_mock = MagicMock()

    db_hook_mock.get_resource_manager.return_value = rm_mock
    get_db_hook_mock.return_value = db_hook_mock
    rm_mock.engines.get_by_name.return_value = engine_mock

    operator_params = {"task_id": "task_id", "engine_name": "engine_name"}

    if operator == "start":
        engine_operator = FireboltStartEngineOperator(**operator_params)
    else:
        engine_operator = FireboltStopEngineOperator(**operator_params)

    engine_operator.execute({})
    get_db_hook_mock.assert_called_once()

    rm_mock.engines.get_by_name.assert_called_once_with("engine_name")

    if operator == "start":
        engine_mock.start.assert_called_once_with(wait_for_startup=True)
    else:
        engine_mock.stop.assert_called_once_with(wait_for_stop=True)
