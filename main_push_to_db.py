from pathlib import Path
import logging
import tempfile
from utilities.utils import extract_zip_to_directory, remove_dir_recursively
import pandas as pd
import os
from logger import setup_logging
from postgres_connector.postgres import Postgres

if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('DefaultLogger.' + __name__)
    temp_path = Path(tempfile.mkdtemp())

    # User defined settings: depending on actual execution method, the settings
    # could be passed as command line arguments or environment variables
    # for example
    settings = {
        "fraudulent_transactions": {
            "directory_compressed_file": "",
            "compressed_filename_prefix": "fraud",
            "compressed_file_format": "zip",
            "decompressed_filename_prefix": "fraud",
            "decompressed_file_format": "csv",
            "fieldnames": ['credit_card_number', 'ipv4', 'state'],
            "target_postgres_table": "fraudulent_transactions"
        },
        "all_transactions": {
            "directory_compressed_file": "",
            "compressed_filename_prefix": "transaction-",
            "compressed_file_format": "zip",
            "decompressed_filename_prefix": "transaction-",
            "decompressed_file_format": "csv",
            "fieldnames": ['credit_card_number', 'ipv4', 'state'],
            "target_postgres_table": "all_transactions"
        }
    }
    # DB settings/secrets would be store in AWS SecretsManager for example
    db_config = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "docker"
    }

    task_to_execute = os.getenv("task_to_execute")
    logger.info(f"Extracting for configuration '{task_to_execute}'")
    # Assert task chosen for execution
    if task_to_execute not in settings.keys():
        raise KeyError(f'The task chosen {task_to_execute} is not in "settings". '
                       f'List of tasks available: {[key for key in settings.keys()]}')

    current_settings = settings[task_to_execute]
    # Assert that the compressed file is in a supported format
    if current_settings["compressed_file_format"] != "zip":
        raise Exception(f'The compreseed file format chosen "{current_settings["compressed_file_format"]}"'
                        f' is not supported currently.')
    # Assert that the decompressed file is in a supported format
    if current_settings["decompressed_file_format"] != "csv":
        raise Exception(f'The decompreseed file format chosen "{current_settings["decompressed_file_format"]}"'
                        f' is not supported currently.')

    # Extract all files into a temporary directory
    list_of_compressed_files = list(
        Path(current_settings["directory_compressed_file"]).glob(
            f'{current_settings["compressed_filename_prefix"]}*.'
            f'{current_settings["compressed_file_format"]}')
    )
    for filepath in list_of_compressed_files:
        extract_zip_to_directory(path_to_zip_file=str(filepath.resolve()), directory_to_extract_to=str(temp_path))
    list_of_decompressed_files = list(Path(temp_path).glob(f'{current_settings["decompressed_filename_prefix"]}*'))

    # Open a Postgres connection
    pg_connection = Postgres(conn_settings=db_config)
    # Copy decompressed files' data into Postgres
    for dec_filepath in list_of_decompressed_files:
        df = pd.read_csv(str(Path(dec_filepath)), skiprows=1, names=settings[task_to_execute]["fieldnames"])
        df.fillna("", inplace=True)
        # In normal conditions I would be testing (at least) that the target table exists
        pg_connection.copy_to_db_from_dataframe(df=df,
                                                target_table=settings[task_to_execute]["target_postgres_table"]
                                                )

    # Recursively remoce temp folder
    remove_dir_recursively(dir_path=temp_path)

    # Closing the Postgres connections
    if pg_connection.conn:
        pg_connection.conn.close()
    logger.info("Execution has finished")
