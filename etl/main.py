import json
import os

from Downloader.downloader import Downloader
from Etl_logger.etl_logger import etl_Logger
from Data_Writer.data_writer import DataWriter

from pytz import timezone
from datetime import datetime

if __name__ == "__main__":
    etl_Logger.info("-----------------  Running Pipeline  --------------------")
    with open(os.getenv("CONFIG_PATH"), "r") as file:
        configs = json.load(file)
    stocks = configs["symbols"]

    isDailyUpdate = configs['isDailyUpdate']

    if isDailyUpdate:
        per = '1d'
        tz = timezone('EST')
        n = datetime.now(tz)
        fromDate = n.strftime("%Y-%m-%d")
    else:
        per = configs["per"]
        fromDate = configs["fromDate"]

    etl_Logger.info(f"Per : {per}, fromDate : {fromDate}")

    dwnloader = Downloader(stocks, per)
    dwnloader.download_files()
    path = dwnloader.get__filepath()
    dw = DataWriter(stocks, path, fromDate)
    dw.run()