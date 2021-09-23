import json
from typing import List

import backoff
import requests
from loguru import logger

from errors import TransformError


def on_backoff(details: dict) -> None:
    logger.error(f'Backing off {details["wait"]:0.1f} seconds after {details["tries"]} tries')


class BaseProcess:
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.ConnectionError,
        on_backoff=on_backoff
    )
    def load(self, transformed_data) -> None:
        data = '\n'.join([json.dumps(item) for item in transformed_data]) + '\n'
        headers = {'Content-Type': 'application/x-ndjson'}
        requests.post('http://0.0.0.0:9200/_bulk', data=data, headers=headers)


class MovieProcess(BaseProcess):

    def extract(self, pg_conn, movie_id):
        with pg_conn.cursor() as cursor:
            cursor.execute("""SELECT
    movie.id,
    movie.title,
    movie.plot,
    movie.imdb_rating
FROM content.movies movie
WHERE movie.id IN (%s);""", (movie_id, ))
            movie = cursor.fetchone()

            cursor.execute("""SELECT
    genre.id,
    genre.name
FROM content.movies movie
LEFT JOIN content.genres_movies gm ON gm.movie_id = movie.id
LEFT JOIN content.genres genre ON genre.id = gm.genre_id
WHERE movie.id IN (%s);""", (movie_id, ))
            genres = cursor.fetchall()

            cursor.execute("""SELECT
    person.id,
    person.name,
    pm.part
FROM content.movies movie
LEFT JOIN content.persons_movies pm ON pm.movie_id = movie.id
LEFT JOIN content.persons person ON person.id = pm.person_id
WHERE movie.id IN (%s);""", (movie_id, ))
            persons = cursor.fetchall()

            return movie, genres, persons

    def transform(self, extracted_data: tuple) -> List[dict]:
        movie, genres, persons = extracted_data

        actors, writers = [], []
        actors_names, writers_names = '', ''
        director = ''
        for person in persons:
            role = person['part']
            if role is None:
                break

            if role == 'a':
                actors.append({'id': person['id'], 'name': person['name']})
                actors_names += person['name']
            elif role == 'w':
                writers.append({'id': person['id'], 'name': person['name']})
                writers_names += person['name']
            elif role == 'd':
                director = person['name']
            else:
                raise TransformError

        genre = ''
        for item in genres:
            genre += item['name']

        result = [{'index': {'_index': 'movies', '_id': movie['id']}}, {
            'title': movie['title'],
            'imdb_rating': movie['imdb_rating'],
            'description': movie['plot'],
            'genre': genre,
            'director': director,
            'actors_names': actors_names,
            'writers_names': writers_names,
            'actors': actors,
            'writers': writers
        }]
        return result
