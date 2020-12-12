# python-streaming-left-join [![uktrade](https://circleci.com/gh/uktrade/python-streaming-left-join.svg?style=shield)](https://circleci.com/gh/uktrade/python-streaming-left-join)

Join iterables in code without loading them all in memory: similar to a SQL left join.

A use case for this is part of streaming pipeline to convert multiple tables of a relational database into a set of dictionaries, which can then be converted to JSON.


## Installation

```bash
pip install python-streaming-left-join
```


## Example: iterables

A single function is exposed: `join`.

```python
import json
from streaming_left_join import join

# Presented as lists, but these can be any iterables

# The "left" iterable
cities = [
    {'city_id': 1, 'name': 'London'},
    {'city_id': 2, 'name': 'Paris'},
]

# The "right" iterables: the key to join on must be in the same order as the "left" iterable
museums = [
    {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
    {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
]
parks = [
    {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.4164},
    {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
]

cities_with_museums_and_parks = join(
    (cities, lambda city: city['city_id']),
    (museums, lambda museum: museum['city_id']),
    (parks, lambda park: park['city_id']),
)

for city, city_museums, city_parks in cities_with_museums_and_parks:
    city_json = json.dumps({
        'id': city['area_id'],
        'name' : city['name'],
        'museums': [
            {'name': museum['name'], 'number_of_exhibits': park['number_of_exhibits']}
            for museum in city_museums
        ],
        'parks': [
            {'name': park['name'], 'area_km_sq': park['area_km_sq']}
            for park in city_parks
        ]
    })
    print(city_json)  # or something like upload the city_dict somewhere
```


## Example: psycopg2

A more realistic use case of `join` is to join the results of (streaming) queries.

```python
import argparse
import contextlib
import json
import psycopg2
import psycopg2.extras
from streaming_left_join import join

parser = argparse.ArgumentParser()
parser.add_argument('--dsn', nargs=1, type=str, required=True)

args = parser.parse_args()
dsn = args.dsn[0]

@contextlib.contextmanager
def get_conn():
    conn = psycopg2.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()

with get_conn() as conn:

    def query(sql, name, buffer_size):
        # Named psycopg2 cursor => server-side postgresql cursor, to avoid loading all results in memory
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name=name) as cur:
            cur.itersize = buffer_size
            cur.arraysize = buffer_size
            cur.execute(sql)
            yield from cur

    # The "left" iterable
    cities = query('SELECT city_id, name FROM cities ORDER BY city_id', 'cities', 200)

    # The "right" iterables: the key to join on must be in the same order as the "left" iterable
    museums = query('SELECT city_id, name, number_of_exhibits FROM museums ORDER BY city_id', 'museums', 200)
    parks = query('SELECT city_id, name, area_km_sq FROM parks ORDER BY city_id', 'parks', 200)

    cities_with_museums_and_parks = join(
        (cities, lambda area: city['city_id']),
        (museums, lambda museum: museum['city_id']),
        (parks, lambda park: park['city_id']),
    )

    for city, city_museums, city_parks in cities_with_museums_and_parks:
        city_json = json.dumps({
            'id': city['city_id'],
            'name' : city['name'],
            'museums': [
                {'name': museum['name'], 'number_of_exhibits': 'number_of_exhibits': number_of_exhibits}
                for museum in city_museums
            ],
            'parks': [
                {'name': park['name'], 'area_km_sq': park['area_km_sq']}
                for park in city_parks
            ]
        })
        print(city_json)  # or something like upload the city_json somewhere
```


## Order of the iterables must be the same

The key to join on _must_ be in the same order in each iterable for this to work properly. If the iterables are the results of SQL queries, this can be acheived by the same `ORDER BY` clause in each of them.

This can be error-prone, but the `join` function detects if on a mistake has been made regarding ordering. If each iterable is not ordered correctly, then at the end of the iteration, there will be unused data in the right iterables. In this case, iterating over the result of `join` will raise an `UnusedDataException` at the end of the iteration.


## The right iterables are yielded as lists

In the above examples, each `city_museums` and `city_parks` are a `list`, rather than some other iterable that doesn't load into memory all at once. In this respect it's not as streaming as it could be. However, the use cases of `join` are where for each item of the left iterable, all the matching right iterable values need to be in memory anyway. If streaming right iterables are required, some other pattern/library will be needed.
