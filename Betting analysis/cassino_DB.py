import sqlite3
import pandas as pd
import numpy as np
from faker import Faker
import os
from datetime import datetime, timedelta


class CassinoDB:
    def __init__(self, n_player, n_betting, n_game):
        self.n_player = n_player
        self.n_betting = n_betting
        self.n_game = n_game
        self.fake = Faker('pt_BR') 
        self.existing_cpf_set = set()

    def _create_connection(self, db_file):
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except sqlite3.Error as e:
            print(e)
        return None

    def _create_players_table(self, conn):
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS Players (
                        player_id INTEGER PRIMARY KEY,
                        CPF TEXT UNIQUE,
                        name TEXT,
                        birth_date DATE,
                        fone TEXT,
                        email TEXT
                        )''')

    def _create_bets_table(self, conn):
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS Bets (
                        id_aposta INTEGER PRIMARY KEY,
                        player_id INTEGER,
                        game_id INTEGER,
                        game_date DATE,
                        bet_value REAL,
                        result INTEGER,
                        FOREIGN KEY (player_id) REFERENCES Players (player_id),
                        FOREIGN KEY (game_id) REFERENCES Games (game_id)
                        )''')

    def _create_games_table(self, conn):
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS Games (
                        game_id INTEGER PRIMARY KEY,
                        name_game TEXT
                        )''')

    def create_database(self, db_file):
        if os.path.exists(db_file):
            conn = self._create_connection(db_file)
            if conn is not None:
                print("Banco de dados já existe. Excluindo dados existentes...")
                cur = conn.cursor()
                cur.execute("DELETE FROM Players;")
                cur.execute("DELETE FROM Bets;")
                cur.execute("DELETE FROM Games;")
                conn.commit()
                conn.close()
                print("Dados excluídos com sucesso!")
            else:
                print("Erro ao criar conexão com o banco de dados.")
        else:
            conn = self._create_connection(db_file)
            if conn is not None:
                print("Banco de dados não encontrado. Criando novo banco de dados...")
                self._create_players_table(conn)
                self._create_bets_table(conn)
                self._create_games_table(conn)
                conn.close()
                print("Banco de dados criado com sucesso!")
            else:
                print("Erro ao criar conexão com o banco de dados.")

    def populate_database(self, db_file):
        conn = self._create_connection(db_file)
        if conn is not None:
            df_player = self._generate_players_df()
            df_bets = self._generate_bets_df(df_player)
            df_games = self._generate_games_df()

            df_player.to_sql('Players', conn, if_exists='append', index=False)
            df_bets.to_sql('Bets', conn, if_exists='append', index=False)
            df_games.to_sql('Games', conn, if_exists='append', index=False)

            conn.close()
            print("Dados inseridos no banco de dados com sucesso!")
        else:
            print("Erro ao criar conexão com o banco de dados.")

    def gerar_cpf_unico(self):
        cpf = self.fake.cpf()
        while cpf in self.existing_cpf_set:
            cpf = self.fake.cpf()
        self.existing_cpf_set.add(cpf)
        return cpf

    def gerar_data_aleatoria(self, start_date, end_date):
        print(start_date)
        print(end_date)

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        return self.fake.date_between(start_date=start_date, end_date=end_date)

    def _generate_players_df(self):

        data_atual = datetime.now()

        # Data de nascimento mínima (80 anos atrás)
        nascimento_minima = data_atual - timedelta(days=83*365)

        # Data de nascimento máxima (18 anos atrás)
        nascimento_maxima = data_atual - timedelta(days=18*365+10)
        
        data_player = {
            'player_id': range(1, self.n_player+1),
            'CPF': [self.gerar_cpf_unico() for _ in range(self.n_player)],
            'name': [self.fake.name() for _ in range(self.n_player)],
            'birth_date': [self.gerar_data_aleatoria(nascimento_minima.date(), nascimento_maxima.date()) for _ in range(self.n_player)],
            'fone': [self.fake.phone_number() for _ in range(self.n_player)],
            'email': [self.fake.email() for _ in range(self.n_player)]
        }
        return pd.DataFrame(data_player)

    def _generate_bets_df(self, df_player):
        datas_ordenadas = sorted([self.fake.date_this_decade() for _ in range(self.n_betting)])
        data_bets = {
            'id_aposta': range(1, self.n_betting + 1),
            'player_id': np.random.randint(1, self.n_player+1, self.n_betting),
            'game_id': np.random.randint(1, 26, self.n_betting),
            'game_date': datas_ordenadas,
            'bet_value': np.round(np.random.uniform(10, 1000, self.n_betting), 2),
            'result': np.random.choice([1, 0], self.n_betting, p=[0.49, 0.51])
        }
        return pd.DataFrame(data_bets)

    def _generate_games_df(self):
        game_names = ['Blackjack', 'Roleta', 'Poker', 'Caça-níqueis']
        data_names = {
            'game_id': range(1, self.n_game+1), 
            'name_game': np.random.choice(game_names, self.n_game, replace=True), 
        }
        return pd.DataFrame(data_names)


