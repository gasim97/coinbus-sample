import os

from dotenv import load_dotenv
from typing import Optional


load_dotenv()


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


def require_env(key: str) -> str:
    value = get_env(key=key)
    if value is None:
        raise ValueError(f"Required environment variable '{key}' is not set")
    return value


def get_env_as_int(key: str, default: Optional[int] = None) -> Optional[int]:
    value = get_env(key=key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_as_bool(key: str, default: Optional[bool] = None) -> Optional[bool]:
    value = get_env(key=key)
    if value is None:
        return default
        
    true_values = {'1', 'true', 'yes', 'on'}
    false_values = {'0', 'false', 'no', 'off'}
    
    value_lower = value.lower()
    if value_lower in true_values:
        return True
    if value_lower in false_values:
        return False
    return default


def get_env_as_list(
    key: str, 
    separator: str = ',', 
    default: Optional[list[str]] = None
) -> Optional[list[str]]:
    value = get_env(key=key)
    if value is None:
        return default
    return [item.strip() for item in value.split(separator) if item.strip()] 