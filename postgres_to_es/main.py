import backoff
import psycopg2
from loguru import logger
from psycopg2.extras import DictCursor

from checkers import Checker
from loader import config
from processes import MovieProcess
from state import JsonFileStorage, State


def on_backoff(details: dict) -> None:
    logger.error(f'Backing off {details["wait"]:0.1f} seconds after {details["tries"]} tries')


class Processor:

    def __init__(self, checkers, processes):
        self.checkers = checkers
        self.process = processes
        self._recently_updated = set()

    @backoff.on_exception(backoff.expo, (
            psycopg2.OperationalError,
            psycopg2.errors.AdminShutdown,
            psycopg2.InterfaceError
    ), on_backoff=on_backoff)
    def start(self):
        logger.info('The processor is running')
        with psycopg2.connect(**config.database.dict(), cursor_factory=DictCursor) as pg_conn:
            for checker in self.checkers:
                for movie_id in checker.get_updated_objects(pg_conn):
                    if movie_id not in self._recently_updated:
                        extracted = self.process.extract(pg_conn, movie_id)
                        transformed = self.process.transform(extracted)
                        self.process.load(transformed)
                        self._recently_updated.add(movie_id)

        pg_conn.close()
        logger.info('The processor is stopped')


if __name__ == '__main__':
    storage = JsonFileStorage('state.json')
    state = State(storage)
    checkers = (
        Checker(state),
    )
    processor = Processor(checkers, MovieProcess())
    processor.start()
