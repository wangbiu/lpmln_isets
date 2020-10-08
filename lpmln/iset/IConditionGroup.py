
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-10-07 18:35
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IConditionGroup.py
"""


class IConditionGroup:
    def __init__(self, icondition_id):
        self.group_icondition_id = icondition_id
        self.group_children = list()
        self.group_parents = list()
        self.group_common_isets = list()
        self.group_descendant_number = 0

    def to_map(self):
        data = dict()
        for v in vars(self):
            data[v] = self.__getattribute__(v)
        return data


    def load_from_map(self, data):
        for v in data:
            self.__setattr__(v, data[v])

    def to_set(self):
        self.group_children = set(self.group_children)
        self.group_parents = set(self.group_parents)

    def to_list(self):
        self.group_children = list(self.group_children)
        self.group_parents = list(self.group_parents)


    def __str__(self):
        data = self.to_map()
        return str(data)


if __name__ == '__main__':
    pass
    