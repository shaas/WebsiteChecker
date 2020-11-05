"""
Unit tests for the Database library
"""

from context import postgre

import os
import hashlib

from dotenv import load_dotenv
from datetime import datetime


class TestDatabase:

    def test_add_entry(self):
        load_dotenv(verbose=True)
        dbname = os.getenv("WC_DB_NAME")
        dbuser = os.getenv("WC_DB_USER")
        dbhost = os.getenv("WC_DB_HOST")
        dbport = os.getenv("WC_DB_PORT")
        dbpass = os.getenv("WC_DB_PASSWORD")

        database = postgre.Database(dbname, dbuser, dbhost, dbport, dbpass)
        database.open_database("test1", "test2")

        utcnow = datetime.utcnow()
        status = 200
        resp_time = 0.123
        reg_set = True
        reg_found = False
        rep_hash = hashlib.sha256("abc".encode("utf-8")).hexdigest()
        database.add_entry(rep_hash, "abc", utcnow, status, resp_time,
                           reg_set, reg_found)

        # check if test1 db got filled correclty
        database.cursor.execute("SELECT * FROM test1 "
                                f"WHERE RepHash = '{rep_hash}'")
        database.con.commit()

        entry = database.cursor.fetchone()

        assert "abc" == entry[1]
        assert rep_hash == entry[0]

        # check if test2 db got filled correctly
        database.cursor.execute("SELECT * FROM test2 "
                                f"WHERE RepHash = '{rep_hash}'")
        database.con.commit()

        entry = database.cursor.fetchone()

        assert utcnow == entry[1]
        assert status == entry[2]
        assert resp_time == entry[3]
        assert reg_set == entry[4]
        assert reg_found == entry[5]

        database.delete_tables()
        database.close_database()
