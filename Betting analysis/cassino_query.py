import sqlite3

class CassinoQuery:
    def __init__(self, db_file):
        self.db_file = db_file

    def _create_connection(self):
        try:
            conn = sqlite3.connect(self.db_file)
            return conn
        except sqlite3.Error as e:
            print(e)
        return None

    def fetch_all_ages(self):
        conn = self._create_connection()
        if conn is not None:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                       (strftime('%Y', 'now') - strftime('%Y', birth_date)) - 
                       (strftime('%m-%d', 'now') < strftime('%m-%d', birth_date)) AS age
                FROM Players;
            """)
            rows = cur.fetchall()
            conn.close()
            return [row[0] for row in rows]
        else:
            print("Erro ao criar conexão com o banco de dados.")
            return []

