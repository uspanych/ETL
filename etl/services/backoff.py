import logging
from functools import wraps
from time import sleep

from psycopg2 import OperationalError

logger = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, max_attempts=5):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный
    экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param max_attempts: максимальное число попыток
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            time_sleep = start_sleep_time
            attempts = 1
            while attempts <= max_attempts:
                try:
                    conn = func(*args, **kwargs)
                    return conn
                except OperationalError or ConnectionError as error:
                    logger.error(f'Error message {error}')

                    if time_sleep >= border_sleep_time:
                        time_sleep = border_sleep_time
                    else:
                        time_sleep += start_sleep_time * 2 ** factor
                    sleep(time_sleep)
                    attempts += 1
            logging.error('Reached the maximum number of attempts')

        return inner

    return func_wrapper
