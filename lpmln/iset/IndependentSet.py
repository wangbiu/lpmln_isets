# -*- encoding: utf-8 -*-
"""
@Time    : 2020/5/11 14:47
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IndependentSet.py
"""

import copy


class IndependentSet:
    def __init__(self):
        self.intersect_sets = list()
        self.union_sets = list()
        self.members = set()

        self.iset_id = -1
        self.iset_key = ""

    def get_iset_key(self):
        if self.iset_key == "":
            cap_names = [str(s) for s in self.intersect_sets]
            union_names = [str(s) for s in self.union_sets]
            self.iset_key = "%d:%s-%s" % (self.get_iset_id(), ",".join(cap_names), ",".join(union_names))

        return self.iset_key

    def set_cap_sets(self, cap_sets):
        self.intersect_sets = list(copy.deepcopy(cap_sets))

    def set_union_sets(self, union_sets):
        self.union_sets = list(copy.deepcopy(union_sets))

    def set_ind_set(self, ind_set):
        self.members = set(copy.deepcopy(ind_set))

    def get_iset_id(self):
        if self.iset_id < 0:
            length = len(self.intersect_sets) + len(self.union_sets)
            bits = ["0"] * length
            for i in self.intersect_sets:
                bits[i] = "1"
            number = "0b" + "".join(bits)
            self.iset_id = int(number, 2)
        return self.iset_id

    def refresh_iset_id(self):
        self.iset_key = ""
        self.iset_id = -1
        self.get_iset_key()

    def __str__(self):
        self.get_iset_key()
        mem_strs = [str(s) for s in self.members]
        return "%s = {%s}" % (self.get_iset_key(), ", ".join(mem_strs))


if __name__ == '__main__':
    pass
