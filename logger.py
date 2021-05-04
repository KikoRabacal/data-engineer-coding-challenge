import logging
import logging.config
import yaml
from pathlib import Path


def setup_logging() -> None:
    """Instantiates a logging configuration if the yaml file is present and can be read. Alternatively instantiates a
    basic logging config
    """
    config_path = Path('_logging.yaml')
    if config_path.exists():
        with config_path.open(mode='r') as file:
            try:
                config_dict = yaml.safe_load(file.read())
                logging.config.dictConfig(config_dict)
            except yaml.parser.ParserError:
                print("Warning: the '_logging.yaml' file couldn't be read, setting up a default logger")
                logging.basicConfig(level=logging.INFO)
