from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class UUIDMixin(BaseModel):
    id: str


class ModifiedMixin(UUIDMixin):
    modified: datetime


class PostgresModel(ModifiedMixin, UUIDMixin):
    pass


class ToElasticModel(UUIDMixin):
    imdb_rating: Optional[float] = None
    genre: Optional[List[str]] = None
    title: str
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[dict]] = None
    writers: Optional[List[dict]] = None
