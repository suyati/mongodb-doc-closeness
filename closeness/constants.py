

ARRAY_CMP_FIELDS = [
    {
        'field': 'a',
        'weight': WEIGHT * 3
    }
]

ARRAY_DICT_CMP_FIELDS = [
    {
        'field': 'following',
        'unique': 'user_id',
        'weight': WEIGHT * .5
    }
]


STRING_CMP_FIELDS = [
    {
        'field': 'relationship_status',
        'weight': WEIGHT * .5
    }

NUM_CMP_FIELDS = [
    {
        'field': 'height',
        'from': -1,
        'to': 1,
        'weight': WEIGHT * .3
    }
]

OUT_PUT_FIELDS = [
    'name',
]
