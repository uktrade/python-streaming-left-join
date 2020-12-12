import unittest

from streaming_left_join import UnusedDataException, join


class TestJoin(unittest.TestCase):

    def test_list(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
        ]
        parks = [
            {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.42},
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
        ]

        cities_with_museums_and_parks = join(
            (cities, lambda city: city['city_id']),
            (museums, lambda museum: museum['city_id']),
            (parks, lambda park: park['city_id']),
        )

        joined = [
            {
                **city,
                'museums': city_museums,
                'parks': city_parks,
            }
            for city, city_museums, city_parks in cities_with_museums_and_parks
        ]
        self.assertEqual(joined, [
            {
                'city_id': 1,
                'name': 'London',
                'museums': [{'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000}],
                'parks': [{'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.42}],
            }, {
                'city_id': 2,
                'name': 'Paris',
                'museums': [{'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000}],
                'parks': [{'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23}],
            }

        ])

    def test_allow_missing(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
        ]
        parks = [
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
        ]

        cities_with_museums_and_parks = join(
            (cities, lambda city: city['city_id']),
            (museums, lambda museum: museum['city_id']),
            (parks, lambda park: park['city_id']),
        )

        joined = [
            {
                **city,
                'museums': city_museums,
                'parks': city_parks,
            }
            for city, city_museums, city_parks in cities_with_museums_and_parks
        ]
        self.assertEqual(joined, [
            {
                'city_id': 1,
                'name': 'London',
                'museums': [],
                'parks': [],
            }, {
                'city_id': 2,
                'name': 'Paris',
                'museums': [{'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000}],
                'parks': [{'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23}],
            }

        ])

    def test_generator(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
        ]
        parks = [
            {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.42},
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
        ]

        def as_generator(items):
            for item in items:
                yield item

        cities_with_museums_and_parks = join(
            (as_generator(cities), lambda city: city['city_id']),
            (as_generator(museums), lambda museum: museum['city_id']),
            (as_generator(parks), lambda park: park['city_id']),
        )

        joined = [
            {
                **city,
                'museums': city_museums,
                'parks': city_parks,
            }
            for city, city_museums, city_parks in cities_with_museums_and_parks
        ]
        self.assertEqual(joined, [
            {
                'city_id': 1,
                'name': 'London',
                'museums': [{'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000}],
                'parks': [{'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.42}],
            }, {
                'city_id': 2,
                'name': 'Paris',
                'museums': [{'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000}],
                'parks': [{'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23}],
            }
        ])

    def test_streaming(self):
        num_cities = 10
        num_cities_yielded = 0
        num_museums_yielded = 0
        num_parks_yielded = 0

        def cities():
            nonlocal num_cities_yielded
            for i in range(0, num_cities):
                num_cities_yielded += 1
                yield {'city_id': i}

        def museums():
            nonlocal num_museums_yielded
            for i in range(0, num_cities):
                num_museums_yielded += 1
                yield {'city_id': i}

        def parks():
            nonlocal num_parks_yielded
            for i in range(0, num_cities):
                num_parks_yielded += 1
                yield {'city_id': i}

        cities_with_museums_and_parks = join(
            (cities(), lambda city: city['city_id']),
            (museums(), lambda museum: museum['city_id']),
            (parks(), lambda museum: museum['city_id']),
        )

        counts = []
        for city, city_museums, city_parks in cities_with_museums_and_parks:
            counts.append((num_cities_yielded, num_museums_yielded, num_parks_yielded))

        # A bit brittle to internals, but we make sure we don't iterate more than once more than yielded
        self.assertEqual(counts, [
            (1, 2, 2),
            (2, 3, 3),
            (3, 4, 4),
            (4, 5, 5),
            (5, 6, 6),
            (6, 7, 7),
            (7, 8, 8),
            (8, 9, 9),
            (9, 10, 10),
            (10, 10, 10),
        ])

    def test_single_unused_data(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
        ]
        parks = [
            {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.41},
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
            {'city_id': 3, 'name': 'Bois de Boulogne', 'area_km_sq':  8.44},
        ]

        cities_with_museums_and_parks = join(
            (cities, lambda city: city['city_id']),
            (museums, lambda museum: museum['city_id']),
            (parks, lambda park: park['city_id']),
        )

        with self.assertRaises(UnusedDataException):
            for city, city_museums, city_parks in cities_with_museums_and_parks:
                pass

    def test_multiple_unused_data(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
        ]
        parks = [
            {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.41},
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
            {'city_id': 3, 'name': 'Bois de Boulogne', 'area_km_sq':  8.44},
            {'city_id': 3, 'name': 'Bois de Boulogne', 'area_km_sq':  8.44},
        ]

        cities_with_museums_and_parks = join(
            (cities, lambda city: city['city_id']),
            (museums, lambda museum: museum['city_id']),
            (parks, lambda park: park['city_id']),
        )

        with self.assertRaises(UnusedDataException):
            for city, city_museums, city_parks in cities_with_museums_and_parks:
                pass

    def test_incorrect_ordering_first(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
            {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
        ]
        parks = [
            {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.41}, 
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
        ]

        cities_with_museums_and_parks = join(
            (cities, lambda city: city['city_id']),
            (museums, lambda museum: museum['city_id']),
            (parks, lambda park: park['city_id']),
        )

        with self.assertRaises(UnusedDataException):
            for city, city_museums, city_parks in cities_with_museums_and_parks:
                pass

    def test_incorrect_ordering_second(self):
        cities = [
            {'city_id': 1, 'name': 'London'},
            {'city_id': 2, 'name': 'Paris'},
        ]
        museums = [
            {'city_id': 1, 'name': 'Science Museum', 'number_of_exhibits': 300000},
            {'city_id': 2, 'name': 'Louvre', 'number_of_exhibits': 380000},
        ]
        parks = [
            {'city_id': 2, 'name': 'Jardin du Luxembourg', 'area_km_sq':  0.23},
            {'city_id': 1, 'name': 'Hyde Park', 'area_km_sq':  1.41},  
        ]

        cities_with_museums_and_parks = join(
            (cities, lambda city: city['city_id']),
            (museums, lambda museum: museum['city_id']),
            (parks, lambda park: park['city_id']),
        )

        with self.assertRaises(UnusedDataException):
            for city, city_museums, city_parks in cities_with_museums_and_parks:
                pass
