import os, logging, datetime
from pathlib import Path

def create_log(string):
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    log_path = os.path.join(Path(__file__).resolve().parent.parent,'logs','debug.log')
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    logger.addHandler(file_handler)
    logger.debug(f'{datetime.datetime.now()} : {string}')