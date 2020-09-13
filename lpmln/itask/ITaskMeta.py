
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 15:03
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ITaskMeta.py
"""


from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.iset.OptimizationISetsUtils as oisu
import itertools
import json
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()


class ITaskMeta:
    """
    rule set tuple: (negative_head (nh), positive_head (h), positive_body (pb), negative_body (nb))
    i3 composed isets: pb \cap nb - ...
    i4 composed isets: h - ...
    i5 composed isets: h \cap nb - ...
    i6 composed isets: h \cap pb - ...
    i7 composed isets: h \cap pb \cap nb - ...
    LPMLN semi-valid rules: i3, or i6, or i7 \neq \emptyset, or i4 = \emptyset
    ASP valid rules: i3, or i6, or i7 \neq \emptyset
    real iset id = iset id + 1
    """
    def __init__(self, kmn, lp_type, is_use_extended_rules):
        print("%d-%d-%d %s itask meta (extended = %s):" % (*kmn, lp_type, str(is_use_extended_rules)))
        self.kmn = kmn
        self.lp_type = lp_type
        self.is_use_extended_rules = is_use_extended_rules
        self.i3_composed_iset_ids = set()
        self.i4_composed_iset_ids = set()
        self.i5_composed_iset_ids = set()
        self.i6_composed_iset_ids = set()
        self.i7_composed_iset_ids = set()

        self.empty_iset_ids = set()
        self.non_se_iset_ids = set()
        self.search_space_iset_ids = set()
        self.search_i4_composed_iset_ids = set()
        self.minmal_i4_isets_tuples = list()


class ITaskMetaGenerator:
    def __init__(self, kmn, lp_type, is_use_extended_rules):
        self.kmn = kmn
        self.lp_type = lp_type
        self.is_use_extended_rules = is_use_extended_rules

        self.meta_data = ITaskMeta(self.kmn, self.lp_type, self.is_use_extended_rules)

        self.rule_number = sum(self.kmn)

        if self.is_use_extended_rules:
            self.rule_set_size = 4
        else:
            self.rule_set_size = 3

        # real_max_iset_id = max_iset_id + 1
        self.max_iset_id = 2 ** (self.rule_set_size * self.rule_number) - 2

        self.init_i_n_composed_iset_ids()
        self.compute_empty_iset_ids()
        self.compute_nse_isets()
        self.compute_search_isets()
        self.compute_search_i4_composed_isets()
        self.compute_min_i4_isets_tuples()

    def init_i_n_composed_iset_ids(self):
        self.meta_data.i3_composed_iset_ids = self.get_i_n_composed_isets(3)
        self.meta_data.i4_composed_iset_ids = self.get_i_n_composed_isets(4)
        self.meta_data.i5_composed_iset_ids = self.get_i_n_composed_isets(5)
        self.meta_data.i6_composed_iset_ids = self.get_i_n_composed_isets(6)
        self.meta_data.i7_composed_iset_ids = self.get_i_n_composed_isets(7)

    def compute_empty_iset_ids(self):
        empty = set()
        if self.rule_number > 1:
            empty = empty.union(self.meta_data.i3_composed_iset_ids)
            empty = empty.union(self.meta_data.i6_composed_iset_ids)
            empty = empty.union(self.meta_data.i7_composed_iset_ids)
            if self.rule_number > 2:
                empty = empty.union(self.meta_data.i5_composed_iset_ids)

        self.meta_data.empty_iset_ids = empty
        print("\t\t has %d empty isets: " % len(empty), empty)

    def get_iset_composition(self, iset_id):
        trans = oct
        if self.is_use_extended_rules:
            trans = hex

        id_bits = trans(iset_id)[2:]
        id_bits = "0" * (self.rule_number - len(id_bits)) + id_bits
        id_bits = [int(b) for b in id_bits]

        return id_bits

    def get_i_n_composed_isets(self, n):
        print("\t\t compute i%d composed iset ids ..." % n )
        isets = set()

        for i in range(self.max_iset_id + 1):
            composition = set(self.get_iset_composition(i + 1))
            if n in composition:
                isets.add(i)

        return isets

    def compute_nse_isets(self):
        print("\t\t compute non-se single iset ids ...")
        validator = ISetConditionValidator(is_use_extended_rules=self.is_use_extended_rules, lp_type=self.lp_type)
        non_se_isets = set()

        k_size = self.kmn[0]
        m_size = self.kmn[1]
        n_size = self.kmn[2]

        for id in range(self.max_iset_id + 1):
            if id not in self.meta_data.empty_iset_ids:
                is_contain_valid_rule, is_se_sat, icondition = validator.validate_isets_kmn_program_from_non_empty_ids(
                {id}, k_size, m_size, n_size, is_check_valid_rule=False)
                if not is_se_sat:
                    non_se_isets.add(id)

        self.meta_data.non_se_iset_ids = non_se_isets
        # print("%s:%d %d-%d-%d itasks find %d non-se isets: \n\t " % (
        # self.lp_type, self.rule_set_size, k_size, m_size, n_size, len(non_se_isets)), non_se_isets)
        print("\t\t has %d non-se isets: " % len(non_se_isets), non_se_isets)

    def compute_search_isets(self):
        print("\t\t compute search space iset ids ...")
        search_isets = set()
        for id in range(self.max_iset_id + 1):
            if id not in self.meta_data.non_se_iset_ids and id not in self.meta_data.empty_iset_ids:
                search_isets.add(id)

        self.meta_data.search_space_iset_ids = search_isets
        print("\t\t has %d search isets: " % len(search_isets), search_isets)

    def check_contain_i4_semi_valid_rule(self, iset_ids):
        rule_number = self.rule_number
        flags = [0] * rule_number
        for id in iset_ids:
            id_bits = self.get_iset_composition(id + 1)
            for i in range(rule_number):
                if id_bits[i] == 4:
                    flags[i] = 1

        if sum(flags) < rule_number:
            return True
        else:
            return False

    def compute_search_i4_composed_isets(self):
        isets = set()
        print("\t\t compute search i4 composed isets ...")
        isets = self.meta_data.search_space_iset_ids.intersection(self.meta_data.i4_composed_iset_ids)
        self.meta_data.search_i4_composed_iset_ids = isets
        print("\t\t has %d search i4 composed isets: " % (len(isets)), isets)

    def compute_min_i4_isets_tuples(self):
        i4_isets_tuples = list()
        iset_ids = self.meta_data.search_i4_composed_iset_ids
        for i in range(1, self.rule_number + 1):
            if i > len(iset_ids):
                break
            counter = itertools.combinations(iset_ids, i)
            for tp in counter:
                tp = set(tp)
                if not self.check_contain_i4_semi_valid_rule(tp):
                    i4_isets_tuples.append(tp)

        skip_tuple_ids = set()
        i4_isets_tuples_ids = [i for i in range(len(i4_isets_tuples))]
        subset_counter = itertools.combinations(i4_isets_tuples_ids, 2)
        for pair in subset_counter:
            pair = list(pair)
            s1 = i4_isets_tuples[pair[0]]
            s2 = i4_isets_tuples[pair[1]]

            if s1.issubset(s2):
                skip_tuple_ids.add(pair[1])
            elif s2.issubset(s1):
                skip_tuple_ids.add(pair[0])

        min_i4_iset_tuples = list()
        for i in i4_isets_tuples_ids:
            if i not in skip_tuple_ids:
                min_i4_iset_tuples.append(i4_isets_tuples[i])

        self.meta_data.minmal_i4_isets_tuples = min_i4_iset_tuples
        print("has %d min i4 iset tuple" % len(min_i4_iset_tuples), min_i4_iset_tuples)


if __name__ == '__main__':
    generator = ITaskMetaGenerator([0, 1, 1], "lpmln", False)
    pass
    