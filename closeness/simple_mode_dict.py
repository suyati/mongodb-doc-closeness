# -*- coding: utf-8 -*-


class SimpleMode():

    def __init__(self, closeness_obj):
        self.ARRAY_CMP_FIELDS = closeness_obj.ARRAY_CMP_FIELDS
        self.ARRAY_DICT_CMP_FIELDS = closeness_obj.ARRAY_DICT_CMP_FIELDS
        self.STRING_CMP_FIELDS = closeness_obj.STRING_CMP_FIELDS
        self.NUM_CMP_FIELDS = closeness_obj.NUM_CMP_FIELDS
        self.cmp_object = self.update_nested_field_to_array(
            closeness_obj.cmp_object)
        self.cmp_objects = closeness_obj.cmp_objects
        self.unit_weight = closeness_obj.unit_weight

    def calcutate_closeness(self):
        for dict_item in self.cmp_objects:
            dict_item = self.update_nested_field_to_array(dict_item)
            dict_item['closeness'] = {'rank': 0, 'weightages': {}}

            dict_item['closeness']['weightages'].update(
                self.set_nested_field_weightages(dict_item)
            )

            dict_item['closeness']['weightages'].update(
                self.set_string_field_weightages(dict_item)
            )
            dict_item['closeness']['weightages'].update(
                self.set_int_field_weightages(dict_item)
            )

            dict_item['closeness']['rank'] = \
                self.calculate_rank(dict_item['closeness']['weightages'])

        return self.cmp_objects

    def set_nested_field_weightages(self, dict_item):
        result = {}
        for var in self.ARRAY_DICT_CMP_FIELDS + self.ARRAY_CMP_FIELDS:
            self_value = self.cmp_object.get(var['field'])
            cmp_value = dict_item.get(var['field'])
            weight = var.get('weight', 1)
            if self_value and cmp_value:
                field_val = self.get_formula(self_value, cmp_value)
                result[var['field']] = self.set_weight(field_val, weight)

        return result

    def set_string_field_weightages(self, dict_item):
        result = {}
        for var in self.STRING_CMP_FIELDS:
            cmp_value = dict_item.get(var['field'])
            self_value = self.cmp_object.get(var['field'])
            weight = var.get('weight', 1)
            if self_value and cmp_value:
                field_val = self.cmp_string(self_value, cmp_value)
                result[var['field']] = self.set_weight(field_val, weight)
        return result

    def set_int_field_weightages(self, dict_item):
        result = {}
        for var in self.NUM_CMP_FIELDS:
            cmp_value = dict_item.get(var['field'])
            self_value = self.cmp_object.get(var['field'])
            weight = var.get('weight', 1)
            if self_value and cmp_value:
                field_val = self.cmp_int(
                    self_value, cmp_value, var['from'], var['to'],
                )
                result[var['field']] = self.set_weight(field_val, weight)

        return result

    def cmp_string(self, self_value, cmp_value):
        return int(self_value == cmp_value)

    def cmp_int(self, self_value, cmp_value, frm, to):
        return int(cmp_value == self_value)

    def calculate_rank(self, weightages):
        rank = 0
        for key, val in weightages.iteritems():
            rank += val

        return rank

    def get_formula(self, self_value, cmp_value):
        """
            weight calculation formula :

             n(A ⋂ B) / n(A)

        """

        setA = set(self_value)
        setB = set(cmp_value)

        nA = float(len(setA))

        if nA == 0:
            return 0

        nAandB = len(setA.intersection(setB))

        return (nAandB / nA)

    def set_weight(self, val, weight):
        return val * weight * self.unit_weight

    def update_nested_field_to_array(self, dict_item):
        for var in self.ARRAY_DICT_CMP_FIELDS:
            result = []
            self_value = dict_item[var['field']]
            for val in self_value:
                result.append(val.get(var['unique']))
            dict_item[var['field']] = result

        return dict_item
