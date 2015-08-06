# -*- coding: utf-8 -*-

from collections import OrderedDict


class ClosenessAggregation():

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
        weight = 0.001
        count = 0
        for option, value in cmp_fields.iteritems():
            setattr(self, option, value)
            for field in value:
                weight += field.get('weight', 1)
                count += 1

        self.unit_weight = 100 / weight

    def get_aggregation_pipeline(self):
        self.generate_pipeline()
        return self.pipeline

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
        for field in self.cmp_object:
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
        query = {'$group': {
            '_id': '$_id',
            'rank': {'$sum': {'$add': []}}
        }}
        query = self.set_nested_field_weightages(query)
        query = self.set_string_field_weightages(query)
        query = self.set_int_field_weightages(query)

        query = self.set_output_fields(query)
        self.pipeline.append(query)

    def set_nested_field_weightages(self, query):
        for var in self.ARRAY_CMP_FIELDS + self.ARRAY_DICT_CMP_FIELDS:
            self_value = self.cmp_object.get(var['field'])
            if self_value:
                query['$group']['rank']['$sum']['$add'].append(
                    self.get_formula(var, self_value)
                )
        return query

    def set_string_field_weightages(self, query):
        for var in self.STRING_CMP_FIELDS:
            self_value = self.cmp_object.get(var['field'])
            if self_value:
                # equation = {'$cond': [conition, true value, false value]}
                equation = {
                    '$cond': [{}, var.get('weight', 1) * self.unit_weight, 0]
                }

                # compare string
                equation['$cond'][0]['$eq'] = [
                    {'$strcasecmp': ["$" + var['field'], self_value]}, 0
                ]

                query['$group']['rank']['$sum']['$add'].append(equation)

        return query

    def set_int_field_weightages(self, query):
        for var in self.NUM_CMP_FIELDS:
            self_value = self.cmp_object.get(var['field'])
            if self_value:
                # equation = {'$cond': [conition, true value, false value]}
                equation = {
                    '$cond': [{}, var.get('weight', 1) * self.unit_weight, 0]
                }
                # check number is between given range
                equation['$cond'][0]['$and'] = [{
                    '$gte': ['$' + var['field'], self_value + var['from']]
                }, {
                    '$lte': ['$' + var['field'], self_value + var['to']]
                }]

                query['$group']['rank']['$sum']['$add'].append(equation)

        return query

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

    def get_formula(self, var, self_value):
        """
            weight calculation formula :

            (( 2 * ( n(A ⋂ B) / n(A) ) + n(A ⋂ B) / n(B) ) / 3 ) * weight


        """

        # n(A)
        nA = len(self_value)

        if nA == 0:
            return {}

        # n(B)     adding 0.001 to avoid divsible by 0 error
        nB = {'$add': [
            {'$size': '$' + var['field']},
            0.001
        ]}

        # n(A ⋂ B)
        nAandB = {'$size': {
            '$setIntersection': [
                self_value,
                '$' + var['field']
            ],
        }}

        # ( 2 * ( n(A ⋂ B) / n(A) )

        Two_nAandBby_nA = {
            '$multiply': [{
                '$divide': [nAandB, nA],
            }, 2],
        }

        # n(A ⋂ B) / n(B)

        nAandBby_nA = {
            '$divide': [nAandB, nB]
        }

        # ( 2 * ( n(A U B) / n(A) ) + n(A ⋂ B) / n(B) ) / 3
        Two_nAandBby_nA_Plus_nAandBby_nA_By_three = {
            '$divide': [
                {'$add': [Two_nAandBby_nA, nAandBby_nA]},
                3
            ],
        }

        # (( 2 * ( n(A ⋂ B) / n(A) ) + n(A ⋂ B) / n(B) ) / 3 ) * weight
        Two_nAandBby_nA_Plus_nAandBby_nA_By_three_Mul_weight = {
            '$multiply': [
                Two_nAandBby_nA_Plus_nAandBby_nA_By_three,
                var.get('weight', 1) * self.unit_weight
            ],
        }

        return Two_nAandBby_nA_Plus_nAandBby_nA_By_three_Mul_weight
