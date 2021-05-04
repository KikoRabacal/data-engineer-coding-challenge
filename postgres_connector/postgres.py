import os
import sys

import pandas as pd
import psycopg2
import logging


class Postgres:
    """Encapsulates methods for interacting with Postgres DBs"""

    def __init__(self, conn_settings: dict) -> None:
        """On instance creation, initialises logger and opens a connection
        to the DB
        """
        self.logger = logging.getLogger('DefaultLogger.' + __name__)
        self.conn = self.__connect(conn_settings)

    def __connect(self, conn_settings: dict) -> None:
        """Internal method to connect to the DB
        :param conn_settings: required settings to establish a connection
        """
        self.conn = None
        try:
            self.logger.debug('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**conn_settings)
        except (Exception, psycopg2.DatabaseError) as database_error:
            print(database_error)
            sys.exit(1)
        self.logger.debug("Connection to PostgreSQL was successful")
        return conn

    def copy_to_db_from_dataframe(self, df: pd.DataFrame, target_table: str) -> None:
        """Copies data from a Pandas dataframe to a target_table. This is done
        by leveraging psycopg2 "copy to table from file"
        :param df: data to be copied into the DB
        :param target_table: name of the target table for the copy
        :return:
        """
        # Save the dataframe to disk
        tmp_df = "./tmp_dataframe.csv"
        df.to_csv(tmp_df, index=False, header=False)
        f = open(tmp_df, 'r')
        cursor = self.conn.cursor()
        try:
            cursor.copy_from(f, target_table, sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            os.remove(tmp_df)
            self.conn.rollback()
            cursor.close()
            print(error)
            sys.exit(1)
        self.logger.info("Copy to Postgres from file was successful")
        cursor.close()
        os.remove(tmp_df)

    def execute_query_full(self, query_txt: str) -> list:
        """
        Returns all results from a SQL query
        :param query_txt: the SQL SELECT query that will return the data of
        interest
        """
        cursor = self.conn.cursor()
        cursor.execute(query_txt)
        result = cursor.fetchall()
        cursor.close()
        return result
