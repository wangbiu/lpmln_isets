
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-06 21:20
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SingleISetSEChecker.py
"""

from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.iset.OptimizationISetsUtils as oisu
import lpmln.config.deprecated.ISCTasksMetaData as meta
import itertools
import json
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()

kmns = [
        [0, 1, 1],
        [1, 1, 0],
        [0, 2, 1],
        [1, 1, 1],
        [1, 2, 0],
        [2, 1, 0],
    ]


def get_non_se_isets(k_size, m_size, n_size, iset_ids, is_use_extended_rules=False, lp_type="lpmln"):
    validator = ISetConditionValidator(is_use_extended_rules=is_use_extended_rules, lp_type=lp_type)
    non_se_isets = list()
    for id in iset_ids:
        is_contain_valid_rule, is_se_sat, icondition = validator.validate_isets_kmn_program_from_non_empty_ids_return_str(
            {id}, k_size, m_size, n_size, is_check_valid_rule=False)
        if not is_se_sat:
            non_se_isets.append(id)

    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    print("%s:%d %d-%d-%d itasks find %d non-se isets: \n\t " % (lp_type, rule_set_size, k_size, m_size, n_size, len(non_se_isets)), non_se_isets)
    return non_se_isets


def get_non_se_test_isets(rule_number, is_use_extended_rules=False):
    empty_iset_ids = oisu.get_empty_indpendent_set_ids(rule_number, is_use_extended_rules)
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3

    iset_number = 2 ** (rule_number * rule_set_size) - 1
    test_iset_ids = list()
    for i in range(iset_number):
        if i not in empty_iset_ids:
            test_iset_ids.append(i)

    print("rule number %d:%d find %d unknown isets \n\t" % (rule_number, rule_set_size, len(test_iset_ids)), test_iset_ids)
    return test_iset_ids


def get_se_single_isets(k_size, m_size, n_size, is_use_extended_rules=False, lp_type="lpmln"):
    rule_number = k_size + m_size + n_size
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    test_iset_ids = get_non_se_test_isets(rule_number, is_use_extended_rules)
    non_se_iset_ids = get_non_se_isets(k_size, m_size, n_size, test_iset_ids, is_use_extended_rules, lp_type)
    se_single_isets = list()
    non_se_iset_ids = set(non_se_iset_ids)
    for i in test_iset_ids:
        if i not in non_se_iset_ids:
            se_single_isets.append(i)

    print("%s:%d %d-%d-%d itasks find %d se isets: \n\t " % (
    lp_type, rule_set_size, k_size, m_size, n_size, len(se_single_isets)), se_single_isets)

    print("------------------------------------------------------------------------------")


def batch_get_se_single_isets(is_use_extended_rules=False, lp_type="lpmln"):
    for kmn in kmns:
        get_se_single_isets(*kmn, is_use_extended_rules=is_use_extended_rules, lp_type=lp_type)


def get_contain_i4_isets(rule_number, is_use_extended_rules=False):
    trans = None
    if is_use_extended_rules:
        rule_set_size = 4
        trans = hex
    else:
        rule_set_size = 3
        trans = oct

    result_log = list()
    i4_iset_ids = dict()
    for i in range(1, rule_number + 1):
        i4_iset_ids[i] = list()
    iset_number = 2 ** (rule_set_size * rule_number) - 1
    for i in range(iset_number):
        bits = trans(i+1)[2:]
        diff = rule_number - len(bits)
        bits = "0" * diff + bits

        i4_cnt = 0
        for b in bits:
            if b == "4":
                i4_cnt += 1
        # print("iset id: %d, trans: %s, contain %d i4 iset" % (i+1, bits, i4_cnt))
        if i4_cnt > 0:
            i4_iset_ids[i4_cnt].append(i)
            result_log.append("%d:%s" % (i, bits))

    # print(i4_iset_ids)
    print(len(result_log), result_log)
    return i4_iset_ids


def get_se_i4_iset_ids(k_size, m_size, n_size, is_use_extended_rules=False):
    rule_number = k_size + m_size + n_size
    i4_iset_ids = get_contain_i4_isets(rule_number, is_use_extended_rules)
    se_iset_ids = meta.get_kmn_isc_meta_data(k_size, m_size, n_size).se_iset_ids
    se_iset_ids = set(se_iset_ids)
    se_i4_iset_ids = dict()
    for key in i4_iset_ids:
        se_ids = list()
        se_i4_iset_ids[key] = se_ids
        for id in i4_iset_ids[key]:
            if id in se_iset_ids:
                se_ids.append(id)
            else:
                # print(id, oct(id+1))
                pass
    print("se i4 iset ids", se_i4_iset_ids)
    return se_i4_iset_ids


def compute_i4_number_from_iset_id(iset_id):
    bits = oct(iset_id + 1)[2:]
    cnt = 0
    for b in bits:
        if b == "4":
            cnt += 1
    return cnt


def compute_i4_number_from_iset_id_list(iset_ids):
    cnt = 0
    for id in iset_ids:
        cnt += compute_i4_number_from_iset_id(id)
    return cnt


def get_oct_iset_id(iset_id, rule_number):
    iset_id = oct(iset_id + 1)[2:]
    diff = rule_number - len(iset_id)
    iset_id = "0"*diff + iset_id
    return iset_id


def check_contain_semi_valid_rule(iset_ids, rule_number):
    flags = [0] * rule_number
    for id in iset_ids:
        id_bits = get_oct_iset_id(id, rule_number)
        for i in range(rule_number):
            if id_bits[i] == "4":
                flags[i] = 1

    if sum(flags) < rule_number:
        return True
    else:
        return False




def get_se_i4_iset_bases(k_size, m_size, n_size, is_use_extended_rules=False):
    se_i4_set_ids = get_se_i4_iset_ids(k_size, m_size, n_size, is_use_extended_rules)
    iset_ids = list()
    for k in se_i4_set_ids:
        iset_ids.extend(se_i4_set_ids[k])
    rule_number = k_size + m_size + n_size
    i4_isets_tuples = list()
    for i in range(1, rule_number + 1):
        if i > len(iset_ids):
            break

        counter = itertools.combinations(iset_ids, i)
        for tuple in counter:
            tuple = set(tuple)
            if not check_contain_semi_valid_rule(tuple, rule_number):
                i4_isets_tuples.append(tuple)
            # print("i4 tuple: ", tuple, " has %d i4 sets" % i4_number)

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

    print("min i4 isets:")
    min_i4_iset_tuples = list()
    for i in i4_isets_tuples_ids:
        if i not in skip_tuple_ids:
            min_i4_iset_tuples.append(i4_isets_tuples[i])
            print(i4_isets_tuples[i], "has %d i4 isets" % compute_i4_number_from_iset_id_list(i4_isets_tuples[i]))

    print("")
    return min_i4_iset_tuples


def batch_generate_min_i4_iset_tuples():
    data = dict()
    i4_key = "i4-tuples"
    for kmn in kmns:
        kmn_data = dict()
        key = "%d-%d-%d" % tuple(kmn)
        print("-----------------%s--------------------" % key)
        data[key] = kmn_data
        tuples = get_se_i4_iset_bases(*kmn)
        tuples = [list(s) for s in tuples]
        kmn_data[i4_key] = tuples

    data_file = config.isc_meta_data_file
    with open(data_file, mode="w", encoding="utf-8")as f:
        json.dump(data, f, indent=2)


if __name__ == '__main__':
    # get_non_se_test_isets(2)
    # get_se_single_isets(0, 1, 1)
    # get_se_single_isets(1, 1, 0)
    # batch_get_se_single_isets()
    # get_contain_i4_isets(2)
    # get_se_i4_iset_ids(1, 1, 1)
    # get_se_i4_iset_bases(0, 2, 1)
    batch_generate_min_i4_iset_tuples()
    pass
    