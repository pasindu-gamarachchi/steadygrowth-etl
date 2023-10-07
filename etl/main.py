import logging
from datetime import  datetime
import time

etl_Logger = logging.getLogger(__name__)
steamHandler = logging.StreamHandler()
timestamp = datetime.now().strftime("%a_%d%-m%-Y_%H%M")
fileHandler = logging.FileHandler(f"logs/{timestamp}.log")
streamFormat = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')

steamHandler.setFormatter(streamFormat)
fileHandler.setFormatter(streamFormat)
steamHandler.setLevel(logging.INFO)
etl_Logger.addHandler(steamHandler)
etl_Logger.addHandler(fileHandler)

etl_Logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    etl_Logger.info("Running")
