from datetime import datetime
from typing import Optional, List, Union
import logging
from psycopg2.extensions import connection as _connection

from etl.services.backoff import backoff
from etl.services.state import JsonFileStorage, State
from .models import PostgresModel, ToElasticModel
from .queries import get_modified_person,  get_modified_filmwork, get_filmwork_by_id, get_modified_genre

logger = logging.getLogger(__name__)


class PostgresExtract:

    def __init__(
            self,
            connection: _connection,
    ):
        self.conn = connection

    @staticmethod
    def set_state(
            file_name: str,
            key: str,
            value: Union[str, datetime],
    ) -> None:
        base = JsonFileStorage(file_name)
        State(base).set_state(
            key,
            value,
        )

    @staticmethod
    def get_state(
            file_name: str,
            key: str,
    ) -> str:
        base = JsonFileStorage(file_name)
        return State(base).get_state(
            key,
        )

    @backoff()
    def execute(
            self,
            query,
    ) -> Optional[List]:
        with self.conn.cursor() as curs:
            curs.execute(query)
            data = curs.fetchall()
            return data

    def modified_persons(
            self,
            bach_size: int,
    ) -> Union[tuple, list]:
        """Метод получает список модифицированных персон."""

        modified_time = self.get_state('person', 'modified') or datetime.min
        response_list = [PostgresModel(**data) for data in self.execute(
            get_modified_person(
                modified_time,
                bach_size,
            )
        )
                         ]
        if len(response_list) >= 1:
            logger.info('Get modified person from Postgres')

            self.set_state(
                'person',
                'modified',
                str(response_list[-1].modified),
            )

            return tuple([item.id for item in response_list]) if len(response_list) > 1 else response_list[0].id
        else:
            return tuple()

    def modified_genres(
            self,
            bach_size: int,
    ) -> Union[tuple, list]:
        """Метод получает список модифицированных жанров."""

        modified_time = self.get_state('genre', 'modified') or datetime.min
        response_list = [PostgresModel(**data) for data in self.execute(
            get_modified_genre(
                modified_time,
                bach_size,
            )
        )
                         ]
        if len(response_list) >= 1:
            logger.info('Get modified genre from Postgres')

            self.set_state(
                'genre',
                'modified',
                str(response_list[-1].modified),
            )

            return tuple([item.id for item in response_list]) if len(response_list) > 1 else response_list[0].id
        else:
            return tuple()

    def modified_filmwork(
            self,
            bach_size: int,
    ) -> Union[tuple, list]:
        """Метод получает список модифицированных фильмов."""

        modified_time = self.get_state('filmwork', 'modified') or datetime.min
        response_list = [PostgresModel(**data) for data in self.execute(
            get_modified_filmwork(
                modified_time,
                bach_size,
            )
        )
                         ]
        if len(response_list) >= 1:
            logger.info('Get modified filmworks from Postgres')

            self.set_state(
                'filmwork',
                'modified',
                str(response_list[-1].modified),
            )

            return tuple([item.id for item in response_list])
        else:
            return tuple()

    def enrich_filmwork_by_id(
            self,
            ids_list: set,
    ) -> List:
        """Метод возвращает список фильмов с дополнительной информацией."""

        if len(ids_list) >= 1:
            logger.info('Enrich modified filmworks')

            filmwork_ids = tuple([item for item in ids_list]) \
                if len(ids_list) > 1 else list(ids_list)[0].id

            response = self.execute(
                get_filmwork_by_id(
                    filmwork_ids,
                )
            )
            return response

    @staticmethod
    def transform_data(
            modified_filmworks: list,
    ) -> List:
        """Метод группирует информацию по фильмам."""

        logger.info('Transform modified filmworks')

        filmworks_list = []

        for filmwork in modified_filmworks:
            actors = []
            actors_names = []
            writers = []
            writers_names = []
            directors = []
            for item in filmwork.get('persons'):
                actor_name = item.get('person_name')
                actor = {
                    'id': item.get('person_id'),
                    'name': item.get('person_name')
                }
                if item.get('person_role') == 'actor':
                    actors_names.append(actor_name)
                    actors.append(actor)
                if item.get('person_role') == 'writer':
                    writers_names.append(actor_name)
                    writers.append(actor)
                if item.get('person_role') == 'director':
                    directors.append(actor_name)

            data = {
                'id': filmwork.get('fw_id'),
                'title': filmwork.get('title'),
                'description': filmwork.get('description'),
                'imdb_rating': filmwork.get('rating'),
                'genre': filmwork.get('genres'),
                'director': directors,
                'actors_names': actors_names,
                'writers_names': writers_names,
                'actors': actors,
                'writers': writers,
            }

            if data not in filmworks_list:
                filmworks_list.append(data)

        return [ToElasticModel(**item) for item in filmworks_list]
