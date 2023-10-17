from Etl_logger.etl_logger import etl_Logger
from Downloader.downloader import Downloader

if __name__ == "__main__":
    etl_Logger.info("Running")
    stocks = ['aapl', 'bp', 'fb', 'googl',  'mdb', 'net', 'nflx',  'shop',   'su',  'team', 'tsla']

    dwnloader = Downloader(stocks, '7d')
    dwnloader.download_files()