# sample data

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
