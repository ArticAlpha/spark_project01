import importlib.util

# Define the path to the config file
# config_path =

def load_config():
    """Dynamically load the config file from a given path."""
    spec = importlib.util.spec_from_file_location("config", "E:\\spark_project01\\resources\\dev\\config.py")
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return config


if __name__ =="__main__":
    config = load_config()