import requests
import json

import backoff


class MoviesProcess:

    def extract(self, pg_conn, date, limit: int = 100, offset: int = 0):
        with pg_conn.cursor() as cursor:
            cursor.execute(
                f'SELECT id, title, plot, imdb_rating, updated_at FROM content.movies WHERE updated_at > %s '
                f'ORDER BY updated_at LIMIT %s OFFSET %s;',
                (date, limit, offset)
            )
            return cursor.fetchall()

    def transform(self, extracted_data: list):
        transformed_data = []
        for row in extracted_data:
            transformed_data.append({'index': {'_index': 'movies', '_id': row['id']}})
            transformed_data.append({
                'title': row['title'],
                'imdb_rating': row['imdb_rating'],
                'description': row['plot']
            })

        return transformed_data

    @backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError)
    def load(self, transformed_data):
        data = '\n'.join([json.dumps(item) for item in transformed_data]) + '\n'
        headers = {'Content-Type': 'application/x-ndjson'}
        requests.post('http://0.0.0.0:9200/_bulk', data=data, headers=headers)
