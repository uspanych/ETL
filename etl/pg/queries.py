from datetime import datetime
from typing import Optional, Union


def get_modified_person(time: datetime) -> Optional[str]:
    """Метод возвращает измененные персоны."""

    return f"""SELECT id, modified
FROM content.person
WHERE modified > '{time}'
ORDER BY modified
LIMIT 150;"""


def get_modified_genre(time: datetime) -> Optional[str]:
    """Метод возвращает id измененных жанров."""

    return f"""SELECT id, modified
FROM content.genre
WHERE modified > '{time}'
ORDER BY modified
LIMIT 150;"""


def get_modified_filmwork(time: datetime) -> Optional[str]:
    """Метод возвращает id измененных фильмов."""

    return f"""SELECT id, modified
FROM content.film_work
WHERE modified > '{time}'
ORDER BY modified
LIMIT 150;"""


def get_filmwork_person(
        persons: Union[str, tuple],
) -> Optional[str]:
    """Метод возвращает id фильмов, с измененными персонами."""

    data = f"IN {persons}" if len(persons) > 1 else f"= '{persons}'"

    return f"""SELECT fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
WHERE pfw.person_id {data}
ORDER BY fw.modified;"""


def get_filmwork_genre(genres: tuple) -> Optional[str]:
    """Метод возвращает id фильмов, с измененными жанрами."""

    data = f"IN {genres}" if len(genres) > 1 else f"= '{genres}'"

    return f"""SELECT fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
WHERE gfw.genre_id {data}
ORDER BY fw.modified;"""


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
    pfw.role, 
    p.id, 
    p.full_name,
    g.name
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id {data}; """
