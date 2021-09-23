import psycopg2
from psycopg2.extras import DictCursor
import backoff
from loguru import logger

from processes import MovieProcess
from checkers import GenreChecker, PersonChecker, MovieChecker
from loader import config
from state import JsonFileStorage, State


def on_backoff(details: dict) -> None:
    logger.error(f'Backing off {details["wait"]:0.1f} seconds after {details["tries"]} tries')


class Processor:

    def __init__(self, checkers, processes):
        self.checkers = checkers
        self.process = processes

    @backoff.on_exception(backoff.expo, (
            psycopg2.OperationalError,
            psycopg2.errors.AdminShutdown,
            psycopg2.InterfaceError
    ), on_backoff=on_backoff)
    def start(self):
        logger.info('The processor is running')
        with psycopg2.connect(**config.database.dict(), cursor_factory=DictCursor) as pg_conn:
            for checker in self.checkers:
                while genre_id := checker.check(pg_conn):
                    for movie_id in checker.get_movie(pg_conn, genre_id):
                        extracted = self.process.extract(pg_conn, movie_id)
                        transformed = self.process.transform(extracted)
                        self.process.load(transformed)

        pg_conn.close()
        logger.info('The processor is stopped')


if __name__ == '__main__':
    storage = JsonFileStorage('state.json')
    state = State(storage)
    checkers = (
        GenreChecker(state),
        PersonChecker(state),
        MovieChecker(state),
    )
    processor = Processor(checkers, MovieProcess())
    processor.start()
