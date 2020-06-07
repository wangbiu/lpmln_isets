# -*- encoding: utf-8 -*-
"""
@Time    : 2020/5/11 14:47
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IndependentSet.py
"""

import copy
import math


class IndependentSet:
    def __init__(self):
        self.intersect_sets = list()
        self.union_sets = list()
        self.members = set()
        self.is_singleton = False

        self.iset_id = -1
        self.iset_key = ""

        self.set_names_template = ["h(%d)", "pb(%d)", "nb(%d)"]
        self.set_names = []

    def get_iset_key(self):
        if self.iset_key == "":
            cap_names = [str(s) for s in self.intersect_sets]
            union_names = [str(s) for s in self.union_sets]
            self.iset_key = "%d:%s-%s" % (self.get_iset_id(), ",".join(cap_names), ",".join(union_names))

        return self.iset_key

    def generate_iset_iusets_from_iset_id(self, id, rule_set_number):
        self.iset_id = id
        id_str = bin(id)[2:]
        id_str = "0" * (rule_set_number - len(id_str)) + id_str
        self.intersect_sets = []
        self.union_sets = []
        for i in range(rule_set_number):
            if id_str[i] == "1":
                self.intersect_sets.append(i)
            else:
                self.union_sets.append(i)

    def set_cap_sets(self, cap_sets):
        self.intersect_sets = list(copy.deepcopy(cap_sets))

    def set_union_sets(self, union_sets):
        self.union_sets = list(copy.deepcopy(union_sets))

    def set_members(self, members, is_set_singleton=False):
        self.members = set(copy.deepcopy(members))
        if is_set_singleton:
            if self.members == 1:
                self.is_singleton = True
            else:
                self.is_singleton = False

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

    def generate_set_names(self):
        rule_set_number = len(self.intersect_sets) + len(self.union_sets)
        rule_number = rule_set_number // 3 + 1
        for i in range(1, rule_number):
            for s in self.set_names_template:
                self.set_names.append(s % i)

    def generate_prefix_op_string(self, sets, op):
        if len(sets) == 0:
            return ""

        if len(sets) == 1:
            return sets[0]

        set_str = sets[0]
        for i in range(1, len(sets)):
            set_str = "%s(%s, %s)" % (op, set_str, sets[i])

        return set_str

    def stringfy_iset_condition_rule(self):
        self.generate_set_names()
        intersect_sets = [self.set_names[i] for i in self.intersect_sets]
        union_sets = [self.set_names[i] for i in self.union_sets]
        i_part = self.generate_prefix_op_string(intersect_sets, "n")
        u_part = self.generate_prefix_op_string(union_sets, "u")

        if u_part == "":
            d_part = i_part
        else:
            d_part = "d(%s, %s)" % (i_part, u_part)

        if len(self.members) == 0:
            iset = "eq(empty_set, %s)." % d_part
        else:
            iset = "ps(empty_set, %s)." % d_part

        if self.is_singleton:
            singleton = "singleton(%s)." % d_part
            iset = "%s \n %s" % (iset, singleton)

        return iset

    def stringfy_iset_condition_tex(self):
        self.generate_set_names()
        intersect_sets = [self.set_names[i] for i in self.intersect_sets]
        union_sets = [self.set_names[i] for i in self.union_sets]
        i_part = " \cap ".join(intersect_sets)
        u_part = " \cup ".join(union_sets)
        iset = "I_{%d} = \\left( %s \\right) - \\left( %s \\right) %%s \\emptyset \n" % (self.get_iset_id(), i_part, u_part)
        if len(self.members) == 0:
            eq = "="
        else:
            eq = "\\neq"
        iset = iset % eq

        if self.is_singleton:
            singleton = "|I_{%d}| = 1" % self.get_iset_id()
            iset = iset + singleton

        return iset

    def __str__(self):
        self.get_iset_key()
        mem_strs = [str(s) for s in self.members]
        return "%s = {%s}" % (self.get_iset_key(), ", ".join(mem_strs))


if __name__ == '__main__':
    pass
