def get_provider_info():
    return {
        "package-name": "airflow-provider-firebolt",
        "name": "Firebolt Provider",
        "description": "A Firebolt provider for Apache Airflow.",
        "hook-class-names": ["firebolt.hooks.firebolt.FireboltHook"],
        "extra-links": ["firebolt.operators.firebolt.RegistryLink"],
        "versions": ["0.0.1"]
        }
