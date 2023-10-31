import json
import os

from Downloader.downloader import Downloader
from Etl_logger.etl_logger import etl_Logger

if __name__ == "__main__":
    etl_Logger.info("Running")
    with open(os.getenv("CONFIG_PATH"), "r") as file:
        configs = json.load(file)

    stocks = configs["symbols"]
    dwnloader = Downloader(stocks, '2mo')
    dwnloader.download_files()