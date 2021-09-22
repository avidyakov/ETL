from datetime import datetime, timedelta

from dateutil import parser


class Checker:
    unprocessed_from_name = 'unprocessed_from_name'
    offset_state_name = 'offset_state_name'

    def __init__(self, connection, state):
        self.connection = connection
        self.state = state
        self.unprocessed_from = self._restore_unprocessed_from()
        self.offset = self._restore_offset()

    def get_unprocessed_from_init(self):
        return datetime.now() - timedelta(days=365 * 10)

    def _restore_unprocessed_from(self):
        if repr_date := self.state.get_state(self.unprocessed_from_name):
            return parser.parse(repr_date)

        return self.get_unprocessed_from_init()

    def _restore_offset(self):
        return self.state.get_state(self.offset_state_name) or 0


class GenreChecker(Checker):
    unprocessed_from_name = 'genre_unprocessed_from'
    offset_state_name = 'genre_offset'

    def get_movie(self, genre_id):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT movie.id FROM content.movies as movie '
                'LEFT JOIN content.genres_movies gm on movie.id = gm.movie_id '
                'WHERE gm.genre_id IN (%s);',
                (genre_id, )
            )

            while row := cursor.fetchone():
                yield row['id']

    def check(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT genre.id, genre.updated_at FROM content.genres genre '
                'WHERE updated_at > %s ORDER BY updated_at OFFSET %s;',
                (self.unprocessed_from, self.offset)
            )
            query_result = cursor.fetchone()
            if query_result is not None:
                genre_updated_at, genre_id = query_result['updated_at'], query_result['id']
                cursor.execute(
                    'SELECT genre.id FROM content.genres genre '
                    'WHERE updated_at > %s ORDER BY updated_at OFFSET %s;',
                    (self.unprocessed_from, self.offset)
                )
                if len(cursor.fetchall()) > 1:
                    self.offset += 1
                    self.state.set_state(self.offset_state_name, self.offset)
                else:
                    self.offset = 0
                    self.state.set_state(self.offset_state_name, self.offset)

                    self.unprocessed_from = genre_updated_at
                    self.state.set_state(self.unprocessed_from_name, str(self.unprocessed_from))

                return genre_id


class PersonChecker(Checker):

    def check(self):
        pass


class MovieChecker(Checker):

    def check(self):
        pass
