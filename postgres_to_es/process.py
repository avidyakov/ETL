import json
from typing import List

import psycopg2
from psycopg2.extras import DictCursor
import backoff
import requests
from loguru import logger

from config import config


def on_backoff(details: dict) -> None:
    logger.error(f'Backing off {details["wait"]:0.1f} seconds after {details["tries"]} tries')


class Process:

    def __init__(self, checkers):
        self.checkers = checkers
        self._recently_updated = set()

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.ConnectionError,
        on_backoff=on_backoff
    )
    def load(self, transformed_data) -> None:
        data = '\n'.join([json.dumps(item) for item in transformed_data]) + '\n'
        headers = {'Content-Type': 'application/x-ndjson'}
        elastic_url = config.elastic.url()
        requests.post(f'{elastic_url}/_bulk', data=data, headers=headers)

    @backoff.on_exception(backoff.expo, (
            psycopg2.OperationalError,
            psycopg2.errors.AdminShutdown,
            psycopg2.InterfaceError
    ), on_backoff=on_backoff)
    def start(self):
        logger.info('The process is running')
        with psycopg2.connect(**config.database.dict(), cursor_factory=DictCursor) as pg_conn:
            for checker in self.checkers:
                for movie_id in checker.get_updated_objects(pg_conn):
                    if movie_id not in self._recently_updated:
                        extracted = self.extract(pg_conn, movie_id)
                        transformed = self.transform(extracted)
                        self.load(transformed)
                        self._recently_updated.add(movie_id)

        pg_conn.close()
        logger.info('The process is stopped')

    def extract(self, pg_conn, movie_id):
        with pg_conn.cursor() as cursor:
            cursor.execute("""SELECT
    m.id as movie_id,
    m.title,
    m.plot,
    m.imdb_rating,
    pm.part,
    p.id as person_id,
    p.name as person_name,
    g.id as genre_id,
    g.name as genre_name
FROM content.movies m
LEFT JOIN content.persons_movies pm ON pm.movie_id = m.id
LEFT JOIN content.persons p ON p.id = pm.person_id
LEFT JOIN content.genres_movies gm ON gm.movie_id = m.id
LEFT JOIN content.genres g ON g.id = gm.genre_id
WHERE m.id IN (%s);""", (movie_id, ))
            return cursor.fetchall()

    def transform(self, extracted_data: tuple) -> List[dict]:
        movie_id = ''
        movie_title = ''
        movie_description = ''
        movie_imdb_rating = ''
        genres = set()
        director = ''
        actors = set()
        writers = set()

        for row in extracted_data:
            if not movie_id and row['movie_id']:
                movie_id = row['movie_id']

            if not movie_title and row['title']:
                movie_title = row['title']

            if not movie_description and row['plot']:
                movie_description = row['plot']

            if not movie_imdb_rating and row['imdb_rating']:
                movie_imdb_rating = row['imdb_rating']

            genre = row['genre_name']
            if genre not in genres:
                genres.add(genre)

            role = row['part']
            if role == 'a':
                actors.add((row['person_id'], row['person_name']))
            elif role == 'w':
                writers.add((row['person_id'], row['person_name']))
            elif role == 'd':
                director = row['person_name']

        return [{'index': {'_index': 'movies', '_id': movie_id}}, {
            'title': movie_title,
            'imdb_rating': movie_imdb_rating,
            'description': movie_description,
            'genre': ' '.join(genres),
            'director': director,
            'actors_names': ' '.join([name for _, name in actors]),
            'writers_names': ' '.join([name for _, name in writers]),
            'actors': [{'id': actor_id, 'name': actor_name} for actor_id, actor_name in actors],
            'writers': [{'id': writer_id, 'name': writer_name} for writer_id, writer_name in writers]
        }]
