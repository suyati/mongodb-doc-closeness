# -*- coding: utf-8 -*-

from collections import OrderedDict


class ClosenessAggregation():

    def __init__(self, user, query, limit, page=1, **options):
        # self.user_obj = user
        self.user = user
        self.page = page
        self.limit = limit
        self.query = query
        self.pipeline = []
        for option, value in options.iteritems():
            setattr(self, option, value)

        # self.tags = tags
        # self.user['details'] = {}
        # if location:
        #     self.user['location'] = location

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
        # query = {}
        # match_settings = self.user['match_settings']
        # distance_in_meter = match_settings['distance'] * 1.60934 / 111.12
        # query['$geoNear'] = {
        #     'near': self.user['location'],
        #     'maxDistance': distance_in_meter,
        #     'includeLocs': "details.location",
        #     'distanceField': "details.distance",
        #     'distanceMultiplier': 111.12 / 1.60934,
        #     'limit': 100000,
        # }
        if self.query:
            self.pipeline.append(self.query)

    # def set_pipeline_stage1_geo_query(self):
    #     query = {}
    #     match_settings = self.user['match_settings']
    #     viewed_user_ids = self.user_obj.get_matched_or_unmatched_user_ids()
    #     query['match'] = {
    #         '_id': {'$nin': viewed_user_ids},
    #         'match_settings.show_on_heatmap': True,
    #         'age': {
    #             '$exists': True,
    #             '$gte': match_settings['age_range']['min_age'],
    #             '$lte': match_settings['age_range']['max_age'],
    #         },
    #     }

    #     query = self.set_gender(query)
    #     query = self.set_tag(query)

    #     self.pipeline[0]['$geoNear']['query'] = query['match']

    def set_pipeline_stage2_project(self):
        self.format_self_user_emb_docs()
        query = {'$project': {}}
        for field in self.user:
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
            self_value = self.user.get(var['field'])
            if self_value:
                query['$group']['rank']['$sum']['$add'].append(
                    self.get_formula(var, self_value)
                )
        return query

    def set_string_field_weightages(self, query):
        for var in self.STRING_CMP_FIELDS:
            self_value = self.user.get(var['field'])
            if self_value:
                # equation = {'$cond': [conition, true value, false value]}
                equation = {'$cond': [{}, var['weight'], 0]}

                # compare string
                equation['$cond'][0]['$eq'] = [
                    {'$strcasecmp': ["$" + var['field'], self_value]}, 0
                ]

                query['$group']['rank']['$sum']['$add'].append(equation)

        return query

    def set_int_field_weightages(self, query):
        for var in self.NUM_CMP_FIELDS:
            self_value = self.user.get(var['field'])
            if self_value:
                # equation = {'$cond': [conition, true value, false value]}
                equation = {'$cond': [{}, var['weight'], 0]}

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

    def set_gender(self, query):
        if self.user['match_settings']['show'] != SETTINGS['GENDERS'][2]:
            query['match']['gender'] = {
                '$exists': True,
                '$eq': self.user['match_settings']['show'],
            }

        return query

    def set_tag(self, query):
        if self.tags:
            query['match']['tags'] = {'$elemMatch': {'$in': self.tags}}

        return query

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
            for doc in self.user[field]:
                ids.append(doc[uniq_id])
            self.user[field] = ids

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
                var['weight']
            ],
        }

        return Two_nAandBby_nA_Plus_nAandBby_nA_By_three_Mul_weight
