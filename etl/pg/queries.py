from datetime import datetime
from typing import Optional, Union


def get_modified_person(
        time: datetime,
        bach_size: int,
) -> Optional[str]:
    """Метод возвращает измененные персоны."""

    return f"""SELECT
                filmwork.id as id,
                person.modified as modified
            FROM content.person person
                INNER JOIN content.person_film_work person_filmwork
                    ON person.id = person_filmwork.person_id
                INNER JOIN content.film_work filmwork
                    ON person_filmwork.film_work_id = filmwork.id
            WHERE
                person.modified > '{time}'
            ORDER BY modified
            LIMIT {bach_size}"""


def get_modified_genre(
        time: datetime,
        bach_size: int,
) -> Optional[str]:
    """Метод возвращает id измененных жанров."""

    return f"""SELECT
                    filmwork.id as id,
                    genre.modified as modified
                FROM content.genre genre
                    INNER JOIN content.genre_film_work genre_filmwork
                        ON genre.id = genre_filmwork.genre_id
                    INNER JOIN content.film_work filmwork
                        ON genre_filmwork.film_work_id = filmwork.id
                WHERE
                    genre.modified > '{time}'
                ORDER BY modified
                LIMIT {bach_size}"""


def get_modified_filmwork(
        time: datetime,
        bach_size: int,
) -> Optional[str]:
    """Метод возвращает id измененных фильмов."""

    return f"""SELECT id, modified
FROM content.film_work
WHERE modified > '{time}'
ORDER BY modified
LIMIT {bach_size};"""


def get_filmwork_by_id(
        ids: Union[str, tuple],
) -> Optional[str]:
    """Метод возвращает фильмы с изменениями."""

    data = f"IN {ids}" if len(ids) > 1 else f"= '{ids}'"

    return f"""SELECT
fw.id as fw_id, 
fw.title, 
fw.description, 
fw.rating, 
fw.type, 
fw.created, 
fw.modified, 
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'person_role', pfw.role,
               'person_id', p.id,
               'person_name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
   ) as persons,
   array_agg(DISTINCT g.name) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id {data}
GROUP BY fw.id;"""
