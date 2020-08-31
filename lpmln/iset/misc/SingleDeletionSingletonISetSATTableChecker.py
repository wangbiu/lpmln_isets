
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-31 13:33
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SingleDeletionSingletonISetSATTableChecker.py
"""

import lpmln.iset.ISetUtils as isu
import copy
from lpmln.iset.misc.SingleAddition2EmptyISetSATTableChecker import SingleAddition2EmptyISetSATTableChecker


class SingleDeletionSingletonISetSATTableChecker(SingleAddition2EmptyISetSATTableChecker):

    # TODO: 未完成
    def generate_all_new_rules(self, rule):
        atoms = isu.get_universe(rule)
        new_rules = list()
        isets = isu.compute_isets_for_sets(rule, self.is_use_extended_rules)
        for at in atoms:
            del_set = {at}
            nr = list()
            for s in rule:
                nr.append(s.difference(del_set))
            new_rules.append(nr)

        return new_rules


if __name__ == '__main__':
    checker = SingleDeletionSingletonISetSATTableChecker(atom_size=3, is_use_extended_rules=True, lp_type="lpmln")
    checker.check_all_rules()
    pass
    