from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
ES_HOST = os.environ.get('ES_HOST')
ES_PORT = os.environ.get('ES_PORT')
ES_INDEX = os.environ.get('ES_INDEX')
ITER_PAUSE = os.environ.get('ITER_PAUSE')
BACH_SIZE = os.environ.get('BACH_SIZE')
