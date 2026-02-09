#!/usr/bin/env python3

import logging
import os
from time import sleep

import psycopg


def main():
    host = os.environ.get("DB_HOST", "sql")
    dbname = os.environ.get("DB_NAME", "ztfref")
    user = os.environ.get("DB_USER", "app")
    conninfo = f"host={host} dbname={dbname} user={user}"

    while True:
        try:
            with psycopg.connect(conninfo) as con:
                with con.cursor() as cur:
                    cur.execute("SELECT 1 FROM refpsfcat LIMIT 0")
            break
        except (psycopg.OperationalError, psycopg.errors.UndefinedTable):
            logging.info("Waiting for postgres to be available")
            sleep(1)

    logging.warning("POSTGRES IS AVAILABLE")


if __name__ == "__main__":
    main()
