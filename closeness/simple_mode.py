
class SimpleMode():

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
            'rank': {'$sum': {'$add': []}}
        }}
        query = self.set_nested_field_weightages(query)
        query = self.set_string_field_weightages(query)
        query = self.set_int_field_weightages(query)

        return query

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
                # check number is equal
                equation['$cond'][0]['$eq'] = [
                    "$" + var['field'], self_value
                ]

                query['$group']['rank']['$sum']['$add'].append(equation)

        return query

    def get_formula(self, var, self_value):
        """
            weight calculation formula :

            n(A ⋂ B) / n(A) * weight


        """

        # n(A)
        nA = len(self_value)

        if nA == 0:
            return {}

        # n(A ⋂ B)
        nAandB = {'$size': {
            '$setIntersection': [
                self_value,
                '$' + var['field']
            ],
        }}

        #  n(A ⋂ B) / n(A)

        nAandBby_nA = {
            '$divide': [nAandB, nA],
        }

        # n(A ⋂ B) / n(A)* weight
        nAandBby_nA_Mul_weight = {
            '$multiply': [
                nAandBby_nA,
                var.get('weight', 1) * self.unit_weight
            ],
        }

        return nAandBby_nA_Mul_weight
