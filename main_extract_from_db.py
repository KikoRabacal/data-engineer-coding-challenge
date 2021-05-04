from logger import setup_logging
from postgres_connector.postgres import Postgres
import logging
import json
from pathlib import Path

if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('DefaultLogger.' + __name__)

    path_for_results_files = '.'
    # DB settings/secrets would be stored in AWS SecretsManager for example
    db_config = {
        "host": "localhost",
        "database": "postgres",
        "user": "postgres",
        "password": "docker"
    }

    # Open a Postgres connection and execute query
    pg_connection = Postgres(conn_settings=db_config)
    query = """
    select LEFT(credit_card_number,LENGTH(credit_card_number)-9)||'*********' 							as masked_credit_card,
        ipv4 																							as ip_address,
        state 																							as state,
        octet_length(LEFT(credit_card_number,LENGTH(credit_card_number)-9)||'*********'||ipv4||state) 	as no_bytes
    from sanitised_transactions
    """
    query_results = pg_connection.execute_query_full(query_txt=query)

    # Save query results to a json file
    results_dict = {}
    entry = 1
    for tupl in query_results:
        results_dict.update({entry: {"col1": tupl[0], "col2": tupl[1], "col3": tupl[2]}})
        entry += 1
    with open(Path(path_for_results_files, "query_results.json"), mode="w") as write_file:
        json.dump(results_dict, write_file)
    # Save query results to binary file
    with open(Path(path_for_results_files, "query_results.bin"), mode="wb") as write_file:
        write_file.write(json.dumps(results_dict).encode())

    # Closing the Postgres connections
    if pg_connection.conn:
        pg_connection.conn.close()
    logger.info("Execution has finished")
