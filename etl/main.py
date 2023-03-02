import logging
import time
from contextlib import closing

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config.settings import DB_NAME, DB_HOST, DB_PASS, DB_USER, DB_PORT, ITER_PAUSE, BACH_SIZE
from es.es_load import ElasticService
from pg.pg_extract import PostgresExtract
from services.backoff import backoff

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s, %(levelname)s, %(message)s, %(name)s",
)

logger = logging.getLogger(__name__)


def etl(
        pg_conn: _connection,
        time_pause: int,
        bach_size: int,
):

    logger.info('Start ETL')
    pg_extract = PostgresExtract(
        pg_conn,
    )
    es_load = ElasticService()

    while True:
        person_ids = pg_extract.modified_persons(bach_size)
        genre_ids = pg_extract.modified_genres(bach_size)
        filmwork_ids = pg_extract.modified_filmwork(bach_size)
        modified_filmworks_ids = set(filmwork_ids + person_ids + genre_ids)

        if len(modified_filmworks_ids) != 0:
            data = pg_extract.enrich_filmwork_by_id(modified_filmworks_ids)

            response = pg_extract.transform_data(data)
            es_load.load_data_to_es(response)

        time.sleep(time_pause)


@backoff()
def pg_connect(
        dsl: dict
):
    with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        etl(
            pg_conn,
            int(ITER_PAUSE),
            BACH_SIZE,
        )


if __name__ == '__main__':
    dsl = {
        'dbname': DB_NAME,
        'user': DB_USER,
        'password': DB_PASS,
        'host': DB_HOST,
        'port': DB_PORT,
    }

    pg_connect(dsl)
