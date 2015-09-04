import unittest

from closeness.closeness_dict import ClosenessDict


class TestQuery(unittest.TestCase):

    def test_dict_fuzzy_mode(self):

        user1 = {
            'name': 'User 1',
            'age': 25,
            'gender': 'male',
            'tags': [
                "tag1",
                "tag2",
                "tag3",
            ],
            'friends': [
                {"user_id": "friend1", 'name': "name1"},
                {"user_id": "friend2", 'name': "name2"},
                {"user_id": "friend3", 'name': "name3"},
            ]
        }
        user2 = {
            'name': 'User 2',
            'age': 25,
            'gender': 'male',
            'tags': [
                "tag1",
                "tag2",
                "tag3",
            ],
            'friends': [
                {"user_id": "friend1", 'name': "name1"},
                {"user_id": "friend2", 'name': "name2"},
                {"user_id": "friend3", 'name': "name3"},
            ]
        }

        user3 = {
            'name': 'User 3',
            'age': 30,
            'gender': 'female',
            'tags': [
                "tag1",
            ],
            'friends': [
                {"user_id": "friend3", 'name': "name3"},
            ]
        }

        ARRAY_CMP_FIELDS = [
            {
                'field': 'tags',
                'weight': 3
            }
        ]

        ARRAY_DICT_CMP_FIELDS = [
            {
                'field': 'friends',
                'unique': 'user_id',
                'weight': .5
            }
        ]

        STRING_CMP_FIELDS = [
            {
                'field': 'gender',
                'weight': .5
            }
        ]

        NUM_CMP_FIELDS = [
            {
                'field': 'age',
                'from': -1,
                'to': 1,
                'weight': .3
            }
        ]

        users = [user2, user3]

        closeness_dict_obj = ClosenessDict(
            user1,
            users,
            ARRAY_CMP_FIELDS=ARRAY_CMP_FIELDS,
            STRING_CMP_FIELDS=STRING_CMP_FIELDS,
            NUM_CMP_FIELDS=NUM_CMP_FIELDS,
            ARRAY_DICT_CMP_FIELDS=ARRAY_DICT_CMP_FIELDS,
        )

        result = closeness_dict_obj.execute(
            mode=ClosenessDict.FUZZY
        )

        self.assertEqual(
            result[0]['closeness']['rank'],
            100.00000000000001)
        self.assertEqual(
            result[1]['closeness']['rank'],
            45.21963824289406)

        # [{'name': 'User 2',
        #   'tags': ['tag1',
        #            'tag2',
        #            'tag3'],
        #   'gender': 'male',
        #   'age': 25,
        #   'closeness': {'weightages': {'gender': 11.627906976744187,
        #                                'age': 6.9767441860465125,
        #                                'friends': 11.627906976744187,
        #                                'tags': 69.76744186046513},
        #                 'rank': 100.00000000000001},
        #   'friends': ['friend1',
        #               'friend2',
        #               'friend3']},
        #  {'name': 'User 3',
        #   'tags': ['tag1'],
        #   'gender': 'female',
        #   'age': 30,
        #   'closeness': {'weightages': {'gender': 0.0,
        #                                'age': 0.0,
        #                                'friends': 6.459948320413436,
        #                                'tags': 38.75968992248062},
        #                 'rank': 45.21963824289406},
        #     'friends': ['friend3']}]

    def test_dict_simple_mode(self):

        user1 = {
            'name': 'User 1',
            'age': 25,
            'gender': 'male',
            'tags': [
                "tag1",
                "tag2",
                "tag3",
            ],
            'friends': [
                {"user_id": "friend1", 'name': "name1"},
                {"user_id": "friend2", 'name': "name2"},
                {"user_id": "friend3", 'name': "name3"},
            ]
        }
        user2 = {
            'name': 'User 2',
            'age': 25,
            'gender': 'male',
            'tags': [
                "tag1",
                "tag2",
                "tag3",
            ],
            'friends': [
                {"user_id": "friend1", 'name': "name1"},
                {"user_id": "friend2", 'name': "name2"},
                {"user_id": "friend3", 'name': "name3"},
            ]
        }

        user3 = {
            'name': 'User 3',
            'age': 30,
            'gender': 'female',
            'tags': [
                "tag1",
            ],
            'friends': [
                {"user_id": "friend3", 'name': "name3"},
            ]
        }

        ARRAY_CMP_FIELDS = [
            {
                'field': 'tags',
                'weight': 3
            }
        ]

        ARRAY_DICT_CMP_FIELDS = [
            {
                'field': 'friends',
                'unique': 'user_id',
                'weight': .5
            }
        ]

        STRING_CMP_FIELDS = [
            {
                'field': 'gender',
                'weight': .5
            }
        ]

        NUM_CMP_FIELDS = [
            {
                'field': 'age',
                'from': -1,
                'to': 1,
                'weight': .3
            }
        ]

        users = [user2, user3]

        closeness_dict_obj = ClosenessDict(
            user1,
            users,
            ARRAY_CMP_FIELDS=ARRAY_CMP_FIELDS,
            STRING_CMP_FIELDS=STRING_CMP_FIELDS,
            NUM_CMP_FIELDS=NUM_CMP_FIELDS,
            ARRAY_DICT_CMP_FIELDS=ARRAY_DICT_CMP_FIELDS,
        )

        result = closeness_dict_obj.execute(
            mode=ClosenessDict.SIMPLE
        )

        self.assertEqual(
            result[0]['closeness']['rank'],
            100.00000000000001)
        self.assertEqual(
            result[1]['closeness']['rank'],
            27.131782945736436)

        # [{'name': 'User 2',
        #   'tags': ['tag1',
        #            'tag2',
        #            'tag3'],
        #   'gender': 'male',
        #   'age': 25,
        #   'closeness': {'weightages': {'gender': 11.627906976744187,
        #                                'age': 6.9767441860465125,
        #                                'friends': 11.627906976744187,
        #                                'tags': 69.76744186046513},
        #                 'rank': 100.00000000000001},
        #   'friends': ['friend1',
        #               'friend2',
        #               'friend3']},
        #  {'name': 'User 3',
        #   'tags': ['tag1'],
        #   'gender': 'female',
        #   'age': 30,
        #   'closeness': {'weightages': {'gender': 0.0,
        #                                'age': 0.0,
        #                                'friends': 3.8759689922480622,
        #                                'tags': 23.255813953488374},
        #                 'rank': 27.131782945736436},
        #     'friends': ['friend3']}]


if __name__ == '__main__':
    unittest.main()
