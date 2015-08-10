# -*- coding: utf-8 -*-


class FuzzyMode():

    """docstring for FuzzyMode"""

    def __init__(self, closeness_obj):
        self.ARRAY_CMP_FIELDS = closeness_obj.ARRAY_CMP_FIELDS
        self.ARRAY_DICT_CMP_FIELDS = closeness_obj.ARRAY_DICT_CMP_FIELDS
        self.STRING_CMP_FIELDS = closeness_obj.STRING_CMP_FIELDS
        self.NUM_CMP_FIELDS = closeness_obj.NUM_CMP_FIELDS
        self.cmp_object = closeness_obj.cmp_object
        self.unit_weight = closeness_obj.unit_weight

    def get_pipeline_stage3_group(self):
        query = {'$group': {
            '_id': '$_id',
            'rank': {'$sum': {'$add': []}},
            'weights': {'$push': {}},
        }}
        self.set_nested_field_weightages(query)
        self.set_string_field_weightages(query)
        self.set_int_field_weightages(query)

        return query

    def set_nested_field_weightages(self, query):
        for var in self.ARRAY_CMP_FIELDS + self.ARRAY_DICT_CMP_FIELDS:
            self_value = self.cmp_object.get(var['field'])
            if self_value:
                field_weight = self.get_formula(var, self_value)
                query['$group']['rank']['$sum']['$add'].append(
                    field_weight
                )
                self.mark_weights(query, var['field'], field_weight)

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
                self.mark_weights(query, var['field'], equation)

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
                self.mark_weights(query, var['field'], equation)

        return query

    def set_field_weight(self, var, self_value):
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

        return Two_nAandBby_nA_Plus_nAandBby_nA_By_three

    def get_formula(self, var, self_value):
        Two_nAandBby_nA_Plus_nAandBby_nA_By_three = self.set_field_weight(
            var,
            self_value
        )

        # (( 2 * ( n(A ⋂ B) / n(A) ) + n(A ⋂ B) / n(B) ) / 3 ) * weight
        Two_nAandBby_nA_Plus_nAandBby_nA_By_three_Mul_weight = {
            '$multiply': [
                Two_nAandBby_nA_Plus_nAandBby_nA_By_three,
                var.get('weight', 1) * self.unit_weight
            ],
        }

        return Two_nAandBby_nA_Plus_nAandBby_nA_By_three_Mul_weight

    def mark_weights(self, query, field, field_weight):
        query['$group']['weights']['$push'][field] = {
            '$add': [
                field_weight,
                0
            ]
        }
        return query
