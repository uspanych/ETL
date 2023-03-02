import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from etl.config.settings import (
    ES_HOST,
    ES_PORT,
    ES_INDEX
)
from etl.pg.models import ToElasticModel
from etl.services.backoff import backoff
from .scheme import MOVIES_SCHEMA

logger = logging.getLogger(__name__)


class ElasticService:
    """Класс загружает пачку данных в Elasticsearch."""

    def __init__(
            self,
    ):
        self.host = ES_HOST
        self.port = int(ES_PORT)
        self.index = ES_INDEX
        self.client = Elasticsearch(
            [
                {
                    'host': self.host, 'port': self.port,
                }
            ]
        )
        self.schema = MOVIES_SCHEMA

    @backoff()
    def check_schema(self):
        if not self.client.ping():
            raise ConnectionError('No connection to Elasticsearch')
        indexes = list(self.client.indices.get_alias().keys())
        if self.index not in indexes:
            logger.info('Index not found ElasticService')

            self.create_index()

    def create_index(self):
        logger.info('Create Index ElasticService')
        self.client.indices.create(index=self.index, body=self.schema)

    @backoff()
    def load_data_to_es(
            self,
            data: list[ToElasticModel],
    ):
        self.check_schema()

        document = [
            {
                "_index": self.index,
                "_id": item.id,
                "_source": item.dict()
            }
            for item in data
        ]
        logger.info('Load data to Elasticsearch')

        bulk(self.client, document)
