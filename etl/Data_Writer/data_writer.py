from datetime import datetime
from sqlalchemy import create_engine

import os
import pandas as pd
from Etl_logger.etl_logger import etl_Logger


class DataWriter:

    def __init__(self, curr_date=None):
        self.curr_date = datetime.now().strftime('%Y-%m-%d') if not curr_date else curr_date
        etl_Logger.info(f"Initialized DataWriter with {self.curr_date}")
        self.engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

    def read_existing_data(self, table_name):
        params = {"symb": table_name, "starting_date": self.curr_date}
        sql_query = pd.read_sql_query(
            f"SELECT * FROM {params['symb']} WHERE `Date` >= %(starting_date)s ORDER BY `Date` DESC;", self.engine,
            params=params)
        data_from_db = pd.DataFrame(sql_query)
        return data_from_db

    def read_from_csv(self, path):
        path += '/data.csv'
        newdf = pd.read_csv(path)
        newdf['Date'] = pd.to_datetime(newdf['Date']).dt.strftime('%Y-%m-%d')
        newdf = newdf[newdf['Date'] > self.curr_date]
        return newdf

    def run(self):
        """
        For each symbol:
            1. Load existing data
            2. Load new data from file
            3. concat dfs
            4. Perform daily percent calcs
            5. Write data to db
        :return:
        """
        pass
        #