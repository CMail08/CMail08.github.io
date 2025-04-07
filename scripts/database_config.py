# database_config.py
import os
import sys
import logging

DB_HOST = os.environ.get("PGHOST", "localhost")
DB_PORT = os.environ.get("PGPORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "springsteen")
HARDCODED_USER = "postgres"
HARDCODED_PASSWORD = "Clemons420!" # <-- Your password hardcoded

def get_connection_string():
    conn_str = (f"dbname='{DB_NAME}' user='{HARDCODED_USER}' host='{DB_HOST}' "
                f"port='{DB_PORT}' password='{HARDCODED_PASSWORD}'")
    logging.debug(f"Generated target DB connection string (using hardcoded credentials)")
    return conn_str

def get_postgres_connection_string():
    conn_str = (f"dbname='postgres' user='{HARDCODED_USER}' host='{DB_HOST}' "
                f"port='{DB_PORT}' password='{HARDCODED_PASSWORD}'")
    logging.debug(f"Generated 'postgres' DB connection string (using hardcoded credentials)")
    return conn_str
