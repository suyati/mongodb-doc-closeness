# -*- coding: utf-8 -*-

from collections import OrderedDict
from fuzzy_mode_dict import FuzzyMode
from simple_mode_dict import SimpleMode


class ClosenessDict():

    (SIMPLE, FUZZY) = (1, 2)
    FIELDS = (
        'ARRAY_CMP_FIELDS',
        'ARRAY_DICT_CMP_FIELDS',
        'STRING_CMP_FIELDS',
        'NUM_CMP_FIELDS',
    )

    def __init__(self, cmp_object, cmp_objects, **cmp_fields):
        self.cmp_object = cmp_object
        self.cmp_objects = list(cmp_objects)
        self.result = {}
        self.set_cmp_feilds(cmp_fields)

    def set_cmp_feilds(self, cmp_fields):
        weight = 0
        for field in self.FIELDS:
            value = cmp_fields.get(field, [])
            setattr(self, field, value)
            for field in value:
                weight += field.get('weight', 1)

        self.unit_weight = 100 / weight

    def execute(self, mode=None):
        self.set_mode(mode)
        return self.mode.calcutate_closeness()

    def set_mode(self, mode):
        if mode == self.SIMPLE:
            self.mode = SimpleMode(self)
        else:
            self.mode = FuzzyMode(self)

    def set_output_fields(self, query):
        for field in self.OUT_PUT_FIELDS:
            self.result[field] = None
