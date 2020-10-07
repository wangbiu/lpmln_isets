
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
        self.group_childern = list()
        self.group_parent_icondition_ids = list()
        self.group_common_isets = list()

    def to_map(self):
        data = dict()
        for v in vars(self):
            data[v] = set.__getattribute__(v)
        return data

if __name__ == '__main__':
    pass
    