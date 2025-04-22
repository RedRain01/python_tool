import os
import yaml
def load_config(key_path: str, default=None) -> any:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "..", "config", "config.yaml")
    config_path = os.path.abspath(config_path)
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception:
        return default

    # 遍历 key_path 路径（如 database.host）
    keys = key_path.split(".")
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    return value