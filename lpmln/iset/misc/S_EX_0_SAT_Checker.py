
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 23:31
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : S_EX_0_SAT_Checker.py
"""

import lpmln.iset.misc.ISetRuleGenerator as ig


def check_all_rules(max_iset_atom_size=2, is_use_extended_rules=False):
    all_rules_isets = ig.generate_all_rules_iset_fmt(max_iset_atom_size, is_use_extended_rules)




if __name__ == '__main__':
    pass
    