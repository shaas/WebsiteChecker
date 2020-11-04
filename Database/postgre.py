import psycopg2

class Database:
    def __init__(self, dbname, dbuser, dbhost, dbport, dbpass):
        self.con = None
        self.cursor = None
        self.wstable = None
        self.wsentries = None
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbpass = dbpass

    def open_database(self, wstable, wsentries):
        if not self.con:
            self.con = psycopg2.connect(database=self.dbname,
                        user=self.dbuser,host=self.dbhost,
                        port=self.dbport, password=self.dbpass)
            self.cursor = self.con.cursor()
        if not self.wstable:
            self.cursor.execute(f"SELECT to_regclass('{wstable}')")
            self.con.commit()
            if not self.cursor.fetchone()[0]:
                self.cursor.execute(f"CREATE TABLE {wstable} (RepHash CHAR(64) PRIMARY KEY, Url VARCHAR(255) NOT NULL);")
                self.con.commit()
            self.wstable = wstable
        if not self.wsentries:
            self.cursor.execute(f"SELECT to_regclass('{wsentries}')")
            self.con.commit()
            if not self.cursor.fetchone()[0]:
                self.cursor.execute(f"CREATE TABLE {wsentries} (RepHash CHAR(64) REFERENCES {self.wstable} ON DELETE CASCADE, Date TIMESTAMP NOT NULL, Status SMALLINT NOT NULL, ResponseTime FLOAT NOT NULL, RegexSet BOOLEAN, RegexFound BOOLEAN, PRIMARY KEY (RepHash, date));")
                self.con.commit()
            self.wsentries = wsentries

    def add_entry(self, rep_hash, url, date, status, response_time, regex_set=False, regex_found=False):
        self.cursor.execute(f"SELECT * FROM {self.wstable} WHERE RepHash = '{rep_hash}'")
        self.con.commit()
        if not self.cursor.fetchone():
            self.cursor.execute(f"INSERT INTO {self.wstable} (RepHash, Url) VALUES ('{rep_hash}', '{url}')")
        self.cursor.execute(f"INSERT INTO {self.wsentries} (RepHash, Date, Status, ResponseTime, RegexSet, RegexFound) VALUES ('{rep_hash}', '{date}', {status}, {response_time}, {regex_set}, {regex_found})")
        self.con.commit()
    
    def close_database(self):
        if self.con:
            self.con.close()
            self.con = None
            self.cursor = None
            self.wstable = None
    
    def delete_tables(self):
        if self.wsentries:
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.wsentries}")
            self.wsentries = None
        if self.wstable:
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.wstable} CASCADE")
            self.wstable = None
        self.con.commit()