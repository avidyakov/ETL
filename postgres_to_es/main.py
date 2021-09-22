import psycopg2
from psycopg2.extras import DictCursor
import backoff

from processes import MovieProcess
from checkers import GenreChecker
from loader import config
from state import JsonFileStorage, State


class Processor:

    def __init__(self, connection, checkers, processes):
        self.connection = connection
        self.checkers = checkers
        self.process = processes

    def start(self):
        for checker in self.checkers:
            while genre_id := checker.check():
                for movie_id in checker.get_movie(genre_id):
                    extracted = self.process.extract(self.connection, movie_id)
                    transformed = self.process.transform(extracted)
                    self.process.load(transformed)


if __name__ == '__main__':
    pg_conn = psycopg2.connect(**config.database.dict(), cursor_factory=DictCursor)
    storage = JsonFileStorage('state.json')
    state = State(storage)
    processor = Processor(pg_conn, (GenreChecker(pg_conn, state), ), MovieProcess())
    processor.start()
    pg_conn.close()
