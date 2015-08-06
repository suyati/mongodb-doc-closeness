import unittest

from closeness.closeness_aggregation import ClosenessAggregation


class TestSimple(unittest.TestCase):

    def test_basic(self):
        user = {
            'name': 'test user100',
            'age': 25,
            'relationship_status': 'test rel',
            'sports': [
                "sports1",
                "sports2",
                "sports3",
            ],
            'friends': [
                {"user_id": "friend1", 'name': "name1"},
                {"user_id": "friend2", 'name': "name2"},
                {"user_id": "friend3", 'name': "name3"},
            ]
        }

        ARRAY_CMP_FIELDS = [
            {
                'field': 'sports',
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
                'field': 'relationship_status',
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
            'name',
        ]

        test = ClosenessAggregation(
            user,
            {},
            OUT_PUT_FIELDS,
            10,
            ARRAY_CMP_FIELDS=ARRAY_CMP_FIELDS,
            STRING_CMP_FIELDS=STRING_CMP_FIELDS,
            NUM_CMP_FIELDS=NUM_CMP_FIELDS,
            ARRAY_DICT_CMP_FIELDS=ARRAY_DICT_CMP_FIELDS,
        )

        query = test.get_aggregation_pipeline()

        self.assertNotEqual(query, [])
        self.assertEqual(len(query), 5)


if __name__ == '__main__':
    unittest.main()
