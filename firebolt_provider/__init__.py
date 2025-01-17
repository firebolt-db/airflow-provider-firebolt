__version__ = "0.5.1"

from typing import Any, Dict


def get_provider_info() -> Dict[str, Any]:
    return {
        "package-name": "airflow-provider-firebolt",
        "name": "Firebolt Provider",
        "description": "A Firebolt provider for Apache Airflow.",
        "hook-class-names": ["firebolt_provider.hooks.firebolt.FireboltHook"],
        "connection-types": [
            {
                "connection-type": "firebolt",
                "hook-class-name": "firebolt_provider.hooks.firebolt.FireboltHook",
            }
        ],
        "extra-links": ["firebolt_provider.operators.firebolt.RegistryLink"],
    }
