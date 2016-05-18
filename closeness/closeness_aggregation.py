# -*- coding: utf-8 -*-

from collections import OrderedDict
from fuzzy_mode import FuzzyMode
from simple_mode import SimpleMode


class ClosenessAggregation():

    (SIMPLE, FUZZY) = (1, 2)
    FIELDS = (
        'ARRAY_CMP_FIELDS',
        'ARRAY_DICT_CMP_FIELDS',
        'STRING_CMP_FIELDS',
        'NUM_CMP_FIELDS',
    )

    def __init__(
            self, cmp_object, query, out_fields, limit, page=1, **cmp_fields):
        self.cmp_object = cmp_object
        self.page = page
        self.limit = limit
        self.query = query
        self.pipeline = []
        self.set_cmp_feilds(cmp_fields)
        self.OUT_PUT_FIELDS = out_fields

    def set_cmp_feilds(self, cmp_fields):
        weight = 0
        for field in self.FIELDS:
            value = cmp_fields.get(field, [])
            setattr(self, field, value)
            for field in value:
                weight += field.get('weight', 1)

        self.unit_weight = 100 / weight

    def get_aggregation_pipeline(self, mode=None):
        self.set_mode(mode)

        self.generate_pipeline()
        return self.pipeline

    def set_mode(self, mode):
        if mode == self.SIMPLE:
            self.mode = SimpleMode(self)
        else:
            self.mode = FuzzyMode(self)

    def generate_pipeline(self):
        self.set_pipeline_stage1_query()
        self.set_pipeline_stage2_project()
        self.set_pipeline_stage3_group()
        self.set_pipeline_stage4_sort()
        self.set_pipeline_limit_and_skip_stage()

    def set_pipeline_stage1_query(self):
        if self.query:
            self.pipeline.append(self.query)

    def set_pipeline_stage2_project(self):
        self.format_self_user_emb_docs()
        query = {'$project': {}}
        for field in self.cmp_object.__class__._fields:
            query['$project'][field] = 1

        for array_dict in self.ARRAY_DICT_CMP_FIELDS:
            field = array_dict['field']
            uniq_id = array_dict['unique']
            query['$project'][field] = {
                '$map': {
                    'input': "$" + field,
                    'as': "temp",
                    'in': "$$temp." + uniq_id
                }
            }

        self.pipeline.append(query)

    def set_pipeline_stage3_group(self):
        query = self.mode.get_pipeline_stage3_group()
        query = self.set_output_fields(query)
        self.pipeline.append(query)

    def set_output_fields(self, query):
        for field in self.OUT_PUT_FIELDS:
            query['$group'][field] = {'$first': '$' + field}
        return query

    def set_pipeline_stage4_sort(self):
        sort_dict = OrderedDict()
        sort_dict['rank'] = -1
        sort_dict['details.distance'] = 1
        sort_dict['_id'] = -1

        query = {'$sort': sort_dict}
        self.pipeline.append(query)

    def set_pipeline_limit_and_skip_stage(self):
        self.pipeline.append(
            {'$skip': (self.page - 1) * self.limit},
        )
        self.pipeline.append(
            {'$limit': self.limit},
        )

    def format_self_user_emb_docs(self):
        for array_dict in self.ARRAY_DICT_CMP_FIELDS:
            uniq_id = array_dict['unique']
            field = array_dict['field']
            ids = []
            for doc in self.cmp_object[field]:
                ids.append(doc[uniq_id])
            self.cmp_object[field] = ids
