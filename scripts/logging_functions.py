''' Configure logging with JSON file and custom logging file'''
import logging
from logging import config
import json
from pathlib import Path


def configure_logging(json_file: Path, log_file: Path) -> None:
    ''' Configure logging with JSON and logging file'''
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            log_conf_json = json.load(f)
    except FileNotFoundError:
        print(f'Error: {json_file} does not exist.')
        return
    except json.JSONDecodeError:
        print('Error: Failed to decode JSON file.')
        return

    handlers = log_conf_json.get('handlers', {})
    for handler in handlers.values():
        if handler.get('class') == 'logging.FileHandler':
            handler['filename'] = str(log_file)
            print(f"Log file set to: {handler['filename']}")

    logging.config.dictConfig(log_conf_json)
    logging.info(f'Logging configuration loaded from {json_file}')


if __name__ == '__main__':
    # Performs basic logging set up and test
    current_file_name = Path(__file__).stem
    log_file_name = f'{current_file_name}_log.log'

    BASE_DIR = Path(__file__).parent.parent
    LOGGING_FILE = BASE_DIR / 'logging_files' / log_file_name
    LOGGING_JSON = BASE_DIR / 'logging_files' / 'logging_config.json'

    configure_logging(LOGGING_JSON, LOGGING_FILE)
    logging.error('Testing saving logs to file.')

    root_logger = logging.getLogger()
    print("\nRoot logger handlers after configuration:")
    for handler in root_logger.handlers:
        print(f"\tHandler: {handler} Level: {handler.level}")