
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-27 16:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SingleAddition2EmptyISetSATTableChecker.py
"""
from lpmln.iset.misc.BaseSATTableChecker import BaseSATTableChecker
import lpmln.iset.misc.SESATTableUtils as seut
import itertools
import copy
from lpmln.sat.LPMLNSAT import LPMLNSAT
from lpmln.sat.ASPSAT import ASPSAT
import lpmln.iset.ISetUtils as isu
from lpmln.utils.counter.BinaryCounter import BinaryCounter


class SingleAddition2EmptyISetSATTableChecker:
    def __init__(self, atom_size, lp_type="lpmln", is_use_extended_rules=False):
        self.atom_size = atom_size
        self.is_use_extended_rules = is_use_extended_rules
        if lp_type == "lpmln":
            self.lp_type = LPMLNSAT
        elif lp_type == "asp":
            self.lp_type = ASPSAT

        if self.is_use_extended_rules:
            self.all_case_flags = ["0000", "1000", "0100", "0010", "1010", "1100", "0110", "1110",
                                   "0001", "1001", "0101", "0011", "1011", "1101", "0111", "1111"]
            self.rule_set_size = 4
        else:
            self.all_case_flags = ["000", "100", "010", "001", "101", "110", "011", "111"]
            self.rule_set_size = 3

    def generate_empty_table(self):
        table = list()
        for i in range(2):
            table.append(set())
        return table

    def generate_000_sat_result(self):
        table = self.generate_empty_table()
        for i in range(2):
            table[i].add(i)

    def check_all_rules(self):
        all_rules = seut.generate_all_rules(self.atom_size, self.is_use_extended_rules)
        results = dict()
        results[self.all_case_flags[0]] = self.generate_000_sat_result()
        for rule in all_rules:
            # print(rule)
            tmp = self.check_one_rule(rule)
            results = self.merge_results(results, tmp)
        self.print_sat_result_table(results)
        return results

    def check_one_rule(self, rule):
        atom_universe = seut.get_atom_set_from_rule(rule)
        results = dict()
        for i in range(len(atom_universe) + 1):
            tit = itertools.combinations(atom_universe, i)
            for tint in tit:
                for j in range(len(tint) + 1):
                    hit = itertools.combinations(tint, j)
                    for hint in hit:
                        tmp = self.check_one_rule_and_seint(rule, set(hint), set(tint))
                        results = self.merge_results(results, tmp)
        return results

    def check_one_rule_and_seint(self, rule, here, there):
        new_atom = self.atom_size + 1
        rule_isets = isu.compute_isets_for_sets(rule, self.is_use_extended_rules)
        results = dict()
        new_rules = self.generate_all_new_rules(rule)
        for key in new_rules:
            if key in results:
                tmp = results[key]
            else:
                tmp = self.generate_empty_table()
                results[key] = tmp

            new_rule = new_rules[key]

            sat_original_rule = self.lp_type.se_satisfy_rule(self.lp_type, here, there, rule)
            sat_new_rule = self.lp_type.se_satisfy_rule(self.lp_type, here, there, new_rule)
            tmp[int(sat_original_rule)].add(int(sat_new_rule))

        return results

    def generate_all_new_rules(self, rule):
        counter = BinaryCounter(self.rule_set_size)
        indicator = counter.get_current_indicator()
        new_atom = self.atom_size + 1
        new_rules = dict()
        while indicator is not None:
            new_rule = seut.deep_copy_rule(rule)
            for i in range(self.rule_set_size):
                if indicator[i] == 1:
                    new_rule[i].add(new_atom)
                    new_rules[counter.get_counter_str(indicator)] = new_rule
            indicator = counter.get_current_indicator()
        return new_rules


    def merge_results(self, old, new):
        for key in new:
            if key not in old:
                old[key] = new[key]
            else:
                old_item = old[key]
                new_item = new[key]
                for i in range(len(old_item)):
                    old_item[i] = old_item[i].union(new_item[i])
        return old

    def print_sat_result_table(self, result_table):
        for key in self.all_case_flags:
            print(key, ": ", result_table[key][1], " | ", result_table[key][0])


if __name__ == '__main__':
    checker = SingleAddition2EmptyISetSATTableChecker(atom_size=3, is_use_extended_rules=True)
    rule = ({0}, {1}, set())
    here = {1}
    there = {0, 1}

    # results = checker.check_one_rule_and_seint(rule, here, there)
    # print(results)


    results = checker.check_all_rules()
    # print(results)
    pass
    