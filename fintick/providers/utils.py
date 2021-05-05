import yaml

from .constants import PROVIDERS


def assert_provider(provider):
    providers = ", ".join(PROVIDERS)
    assert provider in PROVIDERS, f"provider should be one of {providers}"


def get_providers_from_config_yaml():
    try:
        with open("config.yaml", "r") as f:
            data = yaml.safe_load(f)
            for provider in data:
                assert_provider(provider)
            return data
    except FileNotFoundError:
        print("No config.yaml")
