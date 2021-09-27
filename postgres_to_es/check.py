from datetime import datetime, timedelta

from dateutil import parser

CHECKER_QUERY = '''(
    SELECT g.id,
           g.updated_at AS updated_at,
           'genre'      AS type
    FROM content.genres g
    WHERE updated_at > %s
)
UNION ALL
(
    SELECT m.id,
           m.updated_at AS updated_at,
           'movie'      AS type
    FROM content.movies m
    WHERE updated_at > %s
)
UNION ALL
(
    SELECT p.id,
           p.updated_at AS updated_at,
           'person'     AS type
    FROM content.persons p
    WHERE updated_at > %s
)
ORDER BY updated_at
OFFSET %s;'''


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

    def _get_movie_by_genre(self, connection, genre_id):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT movie.id FROM content.movies as movie '
                'LEFT JOIN content.genres_movies gm on movie.id = gm.movie_id '
                'WHERE gm.genre_id IN (%s);',
                (genre_id, )
            )

            while row := cursor.fetchone():
                yield row['id']

    def _get_movie_by_person(self, connection, person_id):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT movie.id FROM content.movies as movie '
                'LEFT JOIN content.persons_movies pm on movie.id = pm.movie_id '
                'WHERE pm.person_id IN (%s);',
                (person_id, )
            )

            while row := cursor.fetchone():
                yield row['id']

    def get_updated_objects(self, connection):
        while check_result := self.check(connection):
            object_id, object_type = check_result
            if object_type == 'movie':
                yield object_id
            elif object_type == 'genre':
                for movie_id in self._get_movie_by_genre(connection, object_id):
                    yield movie_id
            elif object_type == 'person':
                for movie_id in self._get_movie_by_person(connection, object_id):
                    yield movie_id

    def check(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(CHECKER_QUERY, (*(self.unprocessed_from, ) * 3, self.offset))
            query_result = cursor.fetchone()
            if query_result is not None:
                object_updated_at, object_id, object_type = (
                    query_result['updated_at'], query_result['id'], query_result['type']
                )

                cursor.execute(CHECKER_QUERY, (*(self.unprocessed_from, ) * 3, self.offset))
                if len(cursor.fetchall()) > 1:
                    self.offset += 1
                    self.state.set_state(self.offset_state_name, self.offset)
                else:
                    self.offset = 0
                    self.state.set_state(self.offset_state_name, self.offset)

                    self.unprocessed_from = object_updated_at
                    self.state.set_state(self.unprocessed_from_name, str(self.unprocessed_from))
                return object_id, object_type
