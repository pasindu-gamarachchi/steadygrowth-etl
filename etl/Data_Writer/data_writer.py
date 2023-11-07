import logging
from datetime import datetime
from sqlalchemy import create_engine

import os
import pandas as pd
from Etl_logger.etl_logger import etl_Logger
from urllib.parse import quote_plus


class DataWriter:
    __cols_to_keep = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume',
                      'Symbol', 'Nasdaq Traded', 'Security Name', 'Listing Exchange',
                      'Market Category', 'ETF', 'Round Lot Size', 'Test Issue',
                      'Financial Status', 'CQS Symbol', 'NASDAQ Symbol', 'NextShares',
                      'daily_change', 'abs_daily_change']

    def __init__(self, symbs, data_path, curr_date=None):
        self.__curr_date = datetime.now().strftime('%Y-%m-%d') if not curr_date else curr_date
        self.__symbs = symbs
        self.__data_path = data_path
        etl_Logger.info(f"Initialized DataWriter with {self.__curr_date}, {data_path}.")
        DATABASE_PASSWORD_UPDATED = quote_plus(os.getenv('DB_PASS'))

        self.__engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{DATABASE_PASSWORD_UPDATED}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

    def __read_existing_data(self, table_name):
        params = {"symb": table_name, "starting_date": self.__curr_date}
        try:
            sql_query = pd.read_sql_query(
                f"SELECT * FROM {params['symb']} WHERE `Date` >= %(starting_date)s ORDER BY `Date` DESC;", self.__engine,
                params=params)
            data_from_db = pd.DataFrame(sql_query)
            logging.info(f"Read data from {table_name}, df shape {data_from_db.shape[0]}, {data_from_db.shape[1]}")
        except Exception as err:
            logging.error(f"Failed to read data from {table_name}, with {err}")
            return

        return data_from_db

    def __read_from_csv(self, path):
        path += '/data.csv'
        newdf = pd.read_csv(path)
        newdf['Date'] = newdf['Date'].str.split(' ').str[0]
        newdf['Date'] = pd.to_datetime(newdf['Date']).dt.strftime('%Y-%m-%d')
        newdf = newdf[newdf['Date'] > self.__curr_date]
        etl_Logger.info(f"Read data from {path}, df shape {newdf.shape[0]}, {newdf.shape[1]}")
        return newdf

    def __concat_and_prune_cols(self, df1, df2):
        df = pd.concat([df1, df2])
        etl_Logger.info("Concatenated dataframes.")
        return df[self.__cols_to_keep]

    def __change_calculations(self, df):
        df['daily_change'] = df['Close'].pct_change() * 100
        df['abs_daily_change'] = df['Close'].diff()
        etl_Logger.info("Completed change calculations.")
        return df

    def __write_to_db(self, df, symb):
        df = df[df['Date'] != self.__curr_date]
        df.to_sql(symb, self.__engine, if_exists='append', index=False);
        etl_Logger.info(f"Succesfully wrote data for {symb}.")

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
        for symb in self.__symbs:
            etl_Logger.info(f"-------------   Working on symbol : {symb} -------------------------")
            df_db = self.__read_existing_data(symb)
            if not df_db.shape[0]:
                etl_Logger.error(f"Existing data dataframe not returned for {symb}.")
            path = f"{self.__data_path}/{symb}"
            df_from_file = self.__read_from_csv(path)
            df_to_db = self.__concat_and_prune_cols(df_db, df_from_file)
            df_to_db = self.__change_calculations(df_to_db)
            self.__write_to_db(df_to_db, symb)
            etl_Logger.info(f"-------------   Completed symbol : {symb} -------------------------")