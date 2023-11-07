import json
import os

from Downloader.downloader import Downloader
from Etl_logger.etl_logger import etl_Logger
from Data_Writer.data_writer import DataWriter

if __name__ == "__main__":
    etl_Logger.info("-----------------  Running Pipeline  --------------------")
    with open(os.getenv("CONFIG_PATH"), "r") as file:
        configs = json.load(file)
    stocks = configs["symbols"]
    dwnloader = Downloader(stocks, configs["per"])
    dwnloader.download_files()
    dw = DataWriter(stocks, dwnloader.get__filepath(), configs["fromDate"])
    dw.run()