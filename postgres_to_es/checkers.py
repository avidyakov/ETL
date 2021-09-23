from datetime import datetime, timedelta

from dateutil import parser


class Checker:
    unprocessed_from_name = 'unprocessed_from_name'
    offset_state_name = 'offset_state_name'

    def __init__(self, state):
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

    def get_updated_objects(self, connection):
        while genre_id := self.check(connection):
            for movie_id in self.get_movie_id(connection, genre_id):
                yield movie_id

    def get_movie_id(self, connection, genre_id):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT movie.id FROM content.movies as movie '
                'LEFT JOIN content.genres_movies gm on movie.id = gm.movie_id '
                'WHERE gm.genre_id IN (%s);',
                (genre_id, )
            )

            while row := cursor.fetchone():
                yield row['id']

    def check(self, connection):
        with connection.cursor() as cursor:
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
    unprocessed_from_name = 'person_unprocessed_from'
    offset_state_name = 'person_offset'

    def get_updated_objects(self, connection):
        while person_id := self.check(connection):
            for movie_id in self.get_movie(connection, person_id):
                yield movie_id

    def get_movie(self, connection, person_id):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT movie.id FROM content.movies as movie '
                'LEFT JOIN content.persons_movies pm on movie.id = pm.movie_id '
                'WHERE pm.person_id IN (%s);',
                (person_id, )
            )

            while row := cursor.fetchone():
                yield row['id']

    def check(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT person.id, person.updated_at FROM content.persons person '
                'WHERE updated_at > %s ORDER BY updated_at OFFSET %s;',
                (self.unprocessed_from, self.offset)
            )
            query_result = cursor.fetchone()
            if query_result is not None:
                person_updated_at, person_id = query_result['updated_at'], query_result['id']
                cursor.execute(
                    'SELECT person.id FROM content.persons person '
                    'WHERE updated_at > %s ORDER BY updated_at OFFSET %s;',
                    (self.unprocessed_from, self.offset)
                )
                if len(cursor.fetchall()) > 1:
                    self.offset += 1
                    self.state.set_state(self.offset_state_name, self.offset)
                else:
                    self.offset = 0
                    self.state.set_state(self.offset_state_name, self.offset)

                    self.unprocessed_from = person_updated_at
                    self.state.set_state(self.unprocessed_from_name, str(self.unprocessed_from))

                return person_id


class MovieChecker(Checker):
    unprocessed_from_name = 'movie_unprocessed_from'
    offset_state_name = 'movie_offset'

    def get_updated_objects(self, connection):
        while movie_id := self.check(connection):
            yield movie_id

    def check(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT movie.id, movie.updated_at FROM content.movies movie '
                'WHERE updated_at > %s ORDER BY updated_at OFFSET %s;',
                (self.unprocessed_from, self.offset)
            )
            query_result = cursor.fetchone()
            if query_result is not None:
                movie_updated_at, movie_id = query_result['updated_at'], query_result['id']
                cursor.execute(
                    'SELECT movie.id FROM content.movies movie '
                    'WHERE updated_at > %s ORDER BY updated_at OFFSET %s;',
                    (self.unprocessed_from, self.offset)
                )
                if len(cursor.fetchall()) > 1:
                    self.offset += 1
                    self.state.set_state(self.offset_state_name, self.offset)
                else:
                    self.offset = 0
                    self.state.set_state(self.offset_state_name, self.offset)

                    self.unprocessed_from = movie_updated_at
                    self.state.set_state(self.unprocessed_from_name, str(self.unprocessed_from))

                return movie_id
