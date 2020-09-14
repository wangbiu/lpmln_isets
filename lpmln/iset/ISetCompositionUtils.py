
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 10:29
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetCompositionUtils.py
"""


def get_iset_compositions(iset_id, rule_number, is_use_extended_rules):
    trans = oct
    if is_use_extended_rules:
        trans = hex
    id_bits = trans(iset_id)[2:]
    id_bits = "0" * (rule_number - len(id_bits)) + id_bits
    id_bits = [int(b) for b in id_bits]

    return id_bits


def get_i_n_composed_isets(n, rule_number, is_use_extended_rules):
    print("\t\t compute i%d composed iset ids ..." % n)
    isets = set()
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4

    max_iset_id = 2 ** (rule_set_size * rule_number) - 2
    for i in range(max_iset_id + 1):
        composition = set(get_iset_compositions(i + 1, rule_number, is_use_extended_rules))
        if n in composition:
            isets.add(i)

    return isets


def check_contain_rules_without_i_n_iset(iset_n, iset_ids, rule_number, is_use_extended_rules):
    flags = [0] * rule_number
    for id in iset_ids:
        id_bits = get_iset_compositions(id + 1, rule_number, is_use_extended_rules)
        for i in range(rule_number):
            if id_bits[i] == iset_n:
                flags[i] = 1
                break

    if sum(flags) < rule_number:
        return True
    else:
        return False


if __name__ == '__main__':
    pass
    