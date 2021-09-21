from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import DictCursor
import backoff

from processes import MoviesProcess
from loader import config
from state import JsonFileStorage, State


class Processor:

    def __init__(self, state: State, limit: int = 100):
        self.state = state
        self.limit = limit
        self.offset = 0

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def start(self):
        date = datetime.now() - timedelta(days=365 * 100)
        movie_process = MoviesProcess()

        pg_conn = psycopg2.connect(**config.database.dict(), cursor_factory=DictCursor)
        extracted = movie_process.extract(pg_conn, date, self.limit, self.offset)

        while len(extracted):
            transformed = movie_process.transform(extracted)
            movie_process.load(transformed)
            self.offset += self.limit
            extracted = movie_process.extract(pg_conn, date, self.limit, self.offset)

        self.offset = 0
        pg_conn.close()


if __name__ == '__main__':
    storage = JsonFileStorage('states.json')
    state = State(storage)
    processor = Processor(state, limit=100)
    processor.start()
