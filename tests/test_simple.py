import unittest

from closeness.closeness_aggregation import ClosenessAggregation


class TestQuery(unittest.TestCase):

    def test_fuzzy_mode(self):
        from pymongo import MongoClient
        client = MongoClient()
        db = client.test_database
        user_collection = db.user_collection
        user_collection.drop()

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

        user_collection.insert([user1, user2, user3])
        query_stage = {'$match': {'name': {'$ne': user1['name']}}}

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

        OUT_PUT_FIELDS = [
            'name', 'age'
        ]

        # fuzzy mode
        #
        #
        closeness_obj = ClosenessAggregation(
            user1,
            query_stage,
            OUT_PUT_FIELDS,
            10,
            ARRAY_CMP_FIELDS=ARRAY_CMP_FIELDS,
            STRING_CMP_FIELDS=STRING_CMP_FIELDS,
            NUM_CMP_FIELDS=NUM_CMP_FIELDS,
            ARRAY_DICT_CMP_FIELDS=ARRAY_DICT_CMP_FIELDS,
        )

        aggregation_query = closeness_obj.get_aggregation_pipeline(
            mode=ClosenessAggregation.FUZZY
        )

        result = user_collection.aggregate(aggregation_query)

        # {u'ok': 1.0,
        #  u'result': [{u'age': 25,
        #               u'_id': ObjectId('55c894dcb67e20612cd6ddf0'),
        #               u'weights': [{u'gender': 11.627906976744187,
        #                             u'age': 6.9767441860465125,
        #                             u'friends': 11.626615417599819,
        #                             u'tags': 69.75969250559892}],
        #               u'name': u'User 2',
        #               u'rank': 99.99095908598945},
        #              {u'age': 30,
        #               u'_id': ObjectId('55c894dcb67e20612cd6ddf1'),
        #               u'weights': [{u'gender': 0,
        #                             u'age': 0,
        #                             u'friends': 6.456076223518085,
        #                             u'tags': 38.73645734110851}],
        #               u'name': u'User 3',
        #               u'rank': 45.1925335646266}]}

        self.assertNotEqual(aggregation_query, [])
        self.assertEqual(len(aggregation_query), 6)

    def test_simple_mode(self):
        from pymongo import MongoClient
        client = MongoClient()
        db = client.test_database
        user_collection = db.user_collection
        user_collection.drop()

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

        user_collection.insert([user1, user2, user3])
        query_stage = {'$match': {'name': {'$ne': user1['name']}}}

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

        OUT_PUT_FIELDS = [
            'name', 'age'
        ]

        # simple mode
        #
        #

        closeness_obj = ClosenessAggregation(
            user1,
            query_stage,
            OUT_PUT_FIELDS,
            10,
            ARRAY_CMP_FIELDS=ARRAY_CMP_FIELDS,
            STRING_CMP_FIELDS=STRING_CMP_FIELDS,
            NUM_CMP_FIELDS=NUM_CMP_FIELDS,
            ARRAY_DICT_CMP_FIELDS=ARRAY_DICT_CMP_FIELDS,
        )

        aggregation_query = closeness_obj.get_aggregation_pipeline(
            mode=ClosenessAggregation.SIMPLE
        )

        result = user_collection.aggregate(aggregation_query)

        # {u'ok': 1.0,
        #  u'result': [{u'age': 25,
        #               u'_id': ObjectId('55c894dcb67e20612cd6ddf3'),
        #               u'weights': [{u'gender': 11.627906976744187,
        #                             u'age': 6.9767441860465125,
        #                             u'friends': 11.627906976744187,
        #                             u'tags': 69.76744186046513}],
        #               u'name': u'User 2',
        #               u'rank': 100.00000000000001},
        #              {u'age': 30,
        #               u'_id': ObjectId('55c894dcb67e20612cd6ddf4'),
        #               u'weights': [{u'gender': 0,
        #                             u'age': 0,
        #                             u'friends': 3.8759689922480622,
        #                             u'tags': 23.255813953488374}],
        #               u'name': u'User 3',
        #               u'rank': 27.131782945736436}]}

        self.assertNotEqual(aggregation_query, [])
        self.assertEqual(len(aggregation_query), 6)


if __name__ == '__main__':
    unittest.main()
