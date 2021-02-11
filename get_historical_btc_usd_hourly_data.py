import sqlite3


class ExchangeRateDb:
    def __init__(self, database):
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

    def execute(self, query, project):
        self.cursor.execute(query, project)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def create_exchange_rate_table(self):
        query = """
        create table btc_usd_exchange_rate
        (date, close)
        """
        self.cursor.execute(query)
        self.conn.commit()

    def insert_data_from_csv(self):
        with open('gemini_BTCUSD_1hr.csv', 'r') as file:
            next(file)
            for line in file:
                line = line.strip().split(',')
                date_value = line[1]
                close_value = line[-2]
                query = f'insert into btc_usd_exchange_rate values (?, ?)'
                project = (date_value, close_value)
                self.execute(query=query, project=project)
                print(project)


db = ExchangeRateDb('btc_usd_exchange_rate.db')
db.create_exchange_rate_table()
db.insert_data_from_csv()



