from datetime import datetime
from typing import Optional, List, Union
import logging
from psycopg2.extensions import connection as _connection

from etl.services.backoff import backoff
from etl.services.state import JsonFileStorage, State
from .models import PostgresModel, ToElasticModel
from .queries import get_modified_person, get_filmwork_person, get_modified_filmwork, get_filmwork_by_id, \
    get_filmwork_genre, get_modified_genre


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
            self
    ) -> Union[tuple, list]:
        """Метод получает список модифицированных персон."""

        modified_time = self.get_state('person', 'modified') or datetime.min
        response_list = [PostgresModel(**data) for data in self.execute(get_modified_person(modified_time))]
        if len(response_list) >= 1:
            logger.info('Get modified person from Postgres')

            self.set_state(
                'person',
                'modified',
                str(response_list[-1].modified),
            )
            persons_ids = tuple([item.id for item in response_list]) if len(response_list) > 1 else response_list[0].id
            return self.modified_film_works_person(
                persons_ids
            )
        else:
            return tuple()

    def modified_genres(
            self,
    ) -> Union[tuple, list]:
        """Метод получает список модифицированных жанров."""

        modified_time = self.get_state('genre', 'modified') or datetime.min
        response_list = [PostgresModel(**data) for data in self.execute(get_modified_genre(modified_time))]
        if len(response_list) >= 1:
            logger.info('Get modified genre from Postgres')

            self.set_state(
                'genre',
                'modified',
                str(response_list[-1].modified),
            )
            genres_ids = tuple([item.id for item in response_list]) if len(response_list) > 1 else response_list[0].id
            return self.modified_film_works_genre(
                genres_ids,
            )
        else:
            return tuple()

    def modified_filmwork(
            self,
    ) -> Union[tuple, list]:
        """Метод получает список модифицированных фильмов."""

        modified_time = self.get_state('filmwork', 'modified') or datetime.min
        response_list = [PostgresModel(**data) for data in self.execute(get_modified_filmwork(modified_time))]
        if len(response_list) >= 1:
            logger.info('Get modified filmworks from Postgres')

            self.set_state(
                'filmwork',
                'modified',
                str(response_list[-1].modified),
            )
            filmwork_ids = tuple([item.id for item in response_list])
            return filmwork_ids
        else:
            return tuple()

    def modified_film_works_person(
            self,
            mod_ids: tuple,
    ) -> List:
        """Метод отдает список фильмов с модифицированными записями."""

        unique_ids_filmwork = {item.get('id') for item in self.execute(get_filmwork_person(mod_ids))}
        modified_filmworks_ids = list(unique_ids_filmwork)

        filmwork_ids = tuple([item for item in modified_filmworks_ids]) \
            if len(modified_filmworks_ids) > 1 else modified_filmworks_ids[0].id

        return filmwork_ids

    def modified_film_works_genre(
            self,
            mod_ids: tuple,
    ) -> List:
        """Метод отдает список фильмов с модифицированными записями."""

        unique_ids_filmwork = {item.get('id') for item in self.execute(get_filmwork_genre(mod_ids))}
        modified_filmworks_ids = list(unique_ids_filmwork)

        filmwork_ids = tuple([item for item in modified_filmworks_ids]) \
            if len(modified_filmworks_ids) > 1 else modified_filmworks_ids[0].id

        return filmwork_ids

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

        unique_ids = set(item.get('fw_id') for item in modified_filmworks)

        filmworks_list = []

        for fw_id in unique_ids:
            actors = []
            actors_names = []
            writers = []
            writers_names = []
            directors = []
            genres = []
            for item in modified_filmworks:
                if item.get('fw_id') == fw_id:
                    title = item.get('title')
                    description = item.get('description')
                    imdb_rating = item.get('rating')
                    actor_name = item.get('full_name')
                    actor = {
                        'id': item.get('id'),
                        'name': actor_name,
                    }
                    if item.get('name') not in genres:
                        genres.append(item.get('name'))
                    if item.get('role') == 'writer':
                        if actor not in writers:
                            writers.append(actor)
                        if actor_name not in writers_names:
                            writers_names.append(actor_name)
                    if item.get('role') == 'director':
                        if actor_name not in directors:
                            directors.append(actor_name)
                    if item.get('role') == 'actor':
                        if actor not in actors:
                            actors.append(actor)
                        if actor_name not in actors_names:
                            actors_names.append(actor_name)

                    data = {
                        'id': item.get('fw_id'),
                        'imdb_rating': imdb_rating,
                        'genre': genres,
                        'title': title,
                        'description': description,
                        'director': directors,
                        'actors_names': actors_names,
                        'writers_names': writers_names,
                        'actors': actors,
                        'writers': writers,
                    }
                    if data not in filmworks_list:
                        filmworks_list.append(data)

        return [ToElasticModel(**item) for item in filmworks_list]
