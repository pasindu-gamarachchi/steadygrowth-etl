
from Etl_logger.etl_logger import etl_Logger
from pathlib import Path
from datetime import  datetime
import random
import yfinance as yf

class Downloader:

    def __init__(self, stocks_to_dwnload, per):
        self.__stocks_to_dwnload = stocks_to_dwnload
        self.__per = per
        self.__filepath = None

    def download_symb(self, symb, per):
        etl_Logger.info(f" -------  Downloading data, for {symb} for {per}. -----------------")
        timestamp = datetime.now().strftime("%a_%d%-m%-Y_%H%M")
        if not self.__filepath:
            self.__filepath = f"data/{timestamp}"
        self.__make_direc(f"{self.__filepath}/{symb}")
        df = yf.Ticker(symb.upper())
        hist = df.history(period=per)
        etl_Logger.info(f"----------------------      {symb}   ------------------------------")
        etl_Logger.info(f"{hist.head()}")
        etl_Logger.info("-------------------------------------------------------------------")
        hist.to_csv(f'data/{timestamp}/{symb}/data.csv')
        etl_Logger.info(f" -------  Completed downloading data, for {symb} for {per}. -----------------\n")

    def __make_direc(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        etl_Logger.info(f"Making directory for path : {path}")

    def download_files(self):
        for symb in self.__stocks_to_dwnload:
            self.download_symb(symb, self.__per)
            # break

    def get__filepath(self):
        return self.__filepath
