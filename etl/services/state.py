import abc
from typing import Any, Optional
from json.decoder import JSONDecodeError
import json


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище."""

        with open(self.file_path, 'w') as file_state:
            file_state.write(json.dumps(state))

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""
        try:
            with open(self.file_path) as file_state:
                data = json.load(file_state)

            return data
        except (FileNotFoundError, JSONDecodeError):

            return {}


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state(
            {
                key: value,
            }
        )

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        data = self.storage.retrieve_state()

        return data.get(key)
