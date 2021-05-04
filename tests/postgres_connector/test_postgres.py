from postgres_connector.postgres import Postgres
from unittest import mock


def test_connection_created_on_instance_creation() -> None:
    """Tests that upon the creation of an instance of the Postgres class,
    a connection is established (via the __init__ method)
    """
    db_config = {}
    with mock.patch("psycopg2.connect") as mock_connect:
        mock_con = mock_connect.return_value
        instance = Postgres(conn_settings=db_config)
    assert instance.conn == mock_con


def test_execute_query_full() -> None:
    """Tests that the method execute_query_full returns results from a SQL query
    by using cursor.fetchall()
    """
    db_config = {}
    expected_results = [('fake1')]
    with mock.patch("psycopg2.connect") as mock_connect:
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected_results
        conn = Postgres(conn_settings=db_config)
    assert conn.execute_query_full(query_txt='fake query') == expected_results
