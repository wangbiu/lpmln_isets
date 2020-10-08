
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-25 21:23
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISCChecker.py
"""

import lpmln.iset.IConditionUtils as iscu
import lpmln.config.GlobalConfig as cfg
import lpmln.iset.ISetUtils as isu
from lpmln.itask.ITaskMeta import ITaskMeta

config = cfg.load_configuration()


kmn_data = {
    "0-1-0": (0, 1, 0, 1, 7),
    "0-1-1": (0, 1, 1, 1, 16),
    "1-1-0": (1, 1, 0, 1, 20),
    "0-2-1": (0, 2, 1, 1, 33),
    "1-2-0": (1, 2, 0, 1, 42),
    "1-1-1": (1, 1, 1, 1, 45),
    "2-1-0": (2, 1, 0, 1, 54),
}


def process_010_iconditions():
    key = "0-1-0"
    file = config.get_isc_results_file_path(*kmn_data[key])
    conditions = iscu.load_iconditions_from_file(file, False)
    new_file = config.get_isc_results_file_path(*kmn_data[key], "ne")
    with open(new_file, encoding="utf-8", mode="w") as outf:
        for ic in conditions:
            outf.write(str(ic))
            outf.write("\n")


def check_isc_data(kmn_key, type, is_simplify=True):
    print(kmn_key, "iset conditions: ")
    file = config.get_isc_results_file_path(*kmn_data[kmn_key], type)
    conditions = iscu.load_iconditions_from_file(file)
    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets])
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets])
    ignore_ne_isets = set()
    ne_symbols = iscu.get_iconditions_ne_isets_logic_symbols(conditions, ignore_ne_isets)
    print("ne logic symbols: ", ne_symbols)

    # for c in conditions:
    #     print(c, " : ", iscu.convert_icondition_2_conjunctive_formula(c, ne_symbols))

    if is_simplify:
        dnf = iscu.convert_iconditions_2_disjunctive_formula(conditions, ne_symbols)
        print("dnf: ", dnf)
        simp, simp_dnf = iscu.simplify_iconditions(conditions, ignore_ne_isets)
        print("simplified: ", simp)
        print("simplified dnf: ", simp_dnf)



def check_011_programs():
    program = [
        [{3}, set(), {2}],
        [{2, 3}, set(), {2}]
    ]

    isets = isu.compute_isets_from_program(program, False)
    for s in isets:
        print(isets[s])


def check_011_icondition():
    ne_isets = {35}
    icondition_space_isets = {35, 40, 8, 12, 44, 17}
    meta = ITaskMeta.load_itask_meta_data_from_file(config.isc_meta_data_file)
    meta_key = "0-1-1-lpmln-3"
    meta_data = meta[meta_key]
    condition_comp = iscu.complete_and_analyse_icondition(ne_isets, icondition_space_isets, meta_data)

    rule_sets = iscu.get_rule_sets(2, False)
    rule_set_size = 3

    for i in range(len(rule_sets)):
        if i % rule_set_size == 0:
            print("\n")
        key = rule_sets[i]
        print(key, ":", condition_comp[key])


def check_110_icondition():
    ne_isets = {35}
    icondition_space_isets = {0, 1, 4, 8, 12, 17, 32, 35, 36, 40, 44}
    meta = ITaskMeta.load_itask_meta_data_from_file(config.isc_meta_data_file)
    meta_key = "1-1-0-lpmln-3"
    meta_data = meta[meta_key]
    condition_comp = iscu.complete_and_analyse_icondition(ne_isets, icondition_space_isets, meta_data)

    rule_sets = iscu.get_rule_sets(2, False)
    rule_set_size = 3

    for i in range(len(rule_sets)):
        if i % rule_set_size == 0:
            print("\n")
        key = rule_sets[i]
        print(key, ":", condition_comp[key])


def check_021_icondition():
    ne_isets = {291}
    icondition_space_isets = {291, 99, 7, 72, 267, 15, 145, 127, 63}
    meta = ITaskMeta.load_itask_meta_data_from_file(config.isc_meta_data_file)
    meta_key = "2-1-0-lpmln-3"
    meta_data = meta[meta_key]
    condition_comp = iscu.complete_and_analyse_icondition(ne_isets, icondition_space_isets, meta_data)

    rule_sets = iscu.get_rule_sets(3, False)
    rule_set_size = 3

    for i in range(len(rule_sets)):
        if i % rule_set_size == 0:
            print("\n")
        key = rule_sets[i]
        print(key, ":", condition_comp[key])

    file = config.get_isc_results_file_path(*kmn_data["0-2-1"])
    conditions = iscu.load_iconditions_from_file(file)
    iscu.normalize_iconditions(conditions)


def check_120_icondition_no_292():
    file = config.get_isc_results_file_path(*kmn_data["1-2-0"])
    conditions = iscu.load_iconditions_from_file(file)
    # iscu.normalize_iconditions(conditions)
    iscu.count_ne_iset_occurrences(conditions)

    # outf = r"C:\Users\wangb\Desktop\120.csv"
    # iscu.normalize_iconditions(conditions, outf)

    ne_isets = {291}
    conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, ne_isets)
    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    iscu.count_ne_iset_occurrences(conditions)


def check_120_icondition_with_292():
    file = config.get_isc_results_file_path(*kmn_data["1-2-0"])
    conditions = iscu.load_iconditions_from_file(file)
    # iscu.normalize_iconditions(conditions)
    iscu.count_ne_iset_occurrences(conditions)

    # outf = r"C:\Users\wangb\Desktop\120.csv"
    # iscu.normalize_iconditions(conditions, outf)

    ne_isets = {291}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)
    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    print("\n 1st occurrences: ")
    iscu.count_ne_iset_occurrences(conditions)
    print("\n 2nd occurrences: ")
    iscu.count_ne_iset_co_occurrences(conditions, 2)


def check_111_icondition_1():
    file = config.get_isc_results_file_path(*kmn_data["1-1-1"])
    conditions = iscu.load_iconditions_from_file(file)
    # iscu.normalize_iconditions(conditions)
    iscu.count_ne_iset_occurrences(conditions)

    ne_isets = {291, 35, 63, 81, 99, 127, 136, 163}
    conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, ne_isets)
    print("has %d iconditions" % len(conditions))

    outf = r"C:\Users\wangb\Desktop\111-1.csv"
    iscu.normalize_iconditions(conditions, outf)

    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    iscu.count_ne_iset_occurrences(conditions)


def check_111_icondition_2():
    file = config.get_isc_results_file_path(*kmn_data["1-1-1"])
    conditions = iscu.load_iconditions_from_file(file)
    # iscu.normalize_iconditions(conditions)
    # iscu.count_ne_iset_occurrences(conditions)

    ne_isets = {291, 35, 63, 81, 99, 127, 136, 163}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    print("has %d iconditions" % len(conditions))
    iscu.count_ne_iset_occurrences(conditions)

    ne_isets = {35, 63, 81, 99, 127, 136, 163, 255, 257, 271, 273}
    conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, ne_isets)

    outf = r"C:\Users\wangb\Desktop\111.csv"
    iscu.normalize_iconditions(conditions, outf)

    print("has %d iconditions" % len(conditions))

    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    iscu.count_ne_iset_occurrences(conditions)


def check_111_icondition_3():
    file = config.get_isc_results_file_path(*kmn_data["1-1-1"])
    conditions = iscu.load_iconditions_from_file(file)
    total_ne_isets = {1, 2, 8, 9, 10, 16, 17, 18, 36, 64, 73, 82, 100, 128, 137, 146, 164, 256, 258, 265, 268, 272, 274, 289, 292}
    # iscu.normalize_iconditions(conditions)
    # iscu.count_ne_iset_occurrences(conditions)
    ne_isets = {291, 35, 63, 81, 99, 127, 136, 163}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {35, 63, 81, 99, 127, 136, 163, 255, 257, 271, 273}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    print("has %d iconditions" % len(conditions))
    iscu.count_ne_iset_occurrences(conditions)

    total_ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("\ntotal ne isets ", total_ne_isets, len(total_ne_isets))
    ignore_remove_isets = {8, 17, 72, 145}
    # total_ne_isets = total_ne_isets.difference(ignore_remove_isets)
    total_ne_isets = list(total_ne_isets)
    total_ne_isets.sort()
    remove_ne_isets = set()
    for ne in total_ne_isets:
        remove_ne_isets.add(ne)
        print("remove ne ", remove_ne_isets)
        conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, {ne})

        print("has %d iconditions" % len(conditions))

        ne_isets = iscu.get_iconditions_ne_isets(conditions)
        print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
        ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
        print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
        # iscu.count_ne_iset_occurrences(conditions)
        print("\n")


def check_111_icondition_3_1():
    file = config.get_isc_results_file_path(*kmn_data["1-1-1"])
    conditions = iscu.load_iconditions_from_file(file)
    total_ne_isets = {1, 2, 8, 9, 10, 16, 17, 18, 36, 64, 73, 82, 100, 128, 137, 146, 164, 256, 258, 265, 268, 272, 274, 289, 292}
    # iscu.normalize_iconditions(conditions)
    # iscu.count_ne_iset_occurrences(conditions)
    ne_isets = {291, 35, 63, 81, 99, 127, 136, 163}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {35, 63, 81, 99, 127, 136, 163, 255, 257, 271, 273}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    print("has %d iconditions" % len(conditions))
    iscu.count_ne_iset_occurrences(conditions)

    remove_ne_isets = {291, 273, 255, 163, 136, 127, 99, 81, 267, 288}
    print("remove ne ", remove_ne_isets)
    conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, remove_ne_isets)

    outf = r"C:\Users\wangb\Desktop\111.csv"
    iscu.normalize_iconditions(conditions, outf)

    print("has %d iconditions" % len(conditions))

    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    iscu.count_ne_iset_occurrences(conditions)
    print("\n")

def check_111_icondition_3_2():
    file = config.get_isc_results_file_path(*kmn_data["1-1-1"])
    conditions = iscu.load_iconditions_from_file(file)
    total_ne_isets = {1, 2, 8, 9, 10, 16, 17, 18, 36, 64, 73, 82, 100, 128, 137, 146, 164, 256, 258, 265, 268, 272, 274, 289, 292}
    # iscu.normalize_iconditions(conditions)
    # iscu.count_ne_iset_occurrences(conditions)
    ne_isets = {291, 35, 63, 81, 99, 127, 136, 163}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {35, 63, 81, 99, 127, 136, 163, 255, 257, 271, 273}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {291, 273, 255, 163, 136, 127, 99, 81, 267, 288}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    print("has %d iconditions" % len(conditions))
    iscu.count_ne_iset_occurrences(conditions)

    remove_ne_isets = {291, 273, 264, 163, 136, 127, 35}
    print("remove ne ", remove_ne_isets)
    conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, remove_ne_isets)

    outf = r"C:\Users\wangb\Desktop\111.csv"
    iscu.normalize_iconditions(conditions, outf)

    print("has %d iconditions" % len(conditions))

    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    iscu.count_ne_iset_occurrences(conditions)
    print("\n")

def check_111_icondition_3_3():
    file = config.get_isc_results_file_path(*kmn_data["1-1-1"])
    conditions = iscu.load_iconditions_from_file(file)
    ne_isets = {291, 35, 63, 81, 99, 127, 136, 163}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {35, 63, 81, 99, 127, 136, 163, 255, 257, 271, 273}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {291, 273, 255, 163, 136, 127, 99, 81, 267, 288}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    ne_isets = {291, 273, 264, 163, 136, 127, 35}
    conditions = iscu.get_iconditions_contains_at_least_one_of_ne_isets(conditions, ne_isets)

    print("has %d iconditions" % len(conditions))
    iscu.count_ne_iset_occurrences(conditions)

    remove_ne_isets = {291, 163, 35}
    print("remove ne ", remove_ne_isets)
    conditions = iscu.get_iconditions_not_contains_all_ne_isets(conditions, remove_ne_isets)

    outf = r"C:\Users\wangb\Desktop\111.csv"
    iscu.normalize_iconditions(conditions, outf)

    print("has %d iconditions" % len(conditions))

    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", [i + 1 for i in ne_isets], len(ne_isets))
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", [i + 1 for i in ignore_ne_isets], len(ignore_ne_isets))
    iscu.count_ne_iset_occurrences(conditions)
    print("\n")


def group_111_iconditions(type):
    print("compute preliminary_group_kmn_iconditions ... ")
    iscu.preliminary_group_kmn_iconditions(*kmn_data["1-1-1"], type)
    print("compute refine_iconditions_groups ... ")
    iscu.refine_iconditions_groups(*kmn_data["1-1-1"], type)
    print("compute compute_common_isets")
    iscu.compute_common_isets(*kmn_data["1-1-1"], type)


def group_kmn_iconditions(kmn_key, type):
    param = kmn_data[kmn_key]
    print("compute preliminary_group_kmn_iconditions ... ")
    iscu.preliminary_group_kmn_iconditions(*param, type)
    print("compute refine_iconditions_groups ... ")
    iscu.refine_iconditions_groups(*param, type)
    print("compute compute_common_isets")
    iscu.compute_common_isets(*param, type)


def find_021_max_clique():
    param = kmn_data["0-2-1"]
    iscu.find_max_clique(*param, "")


def group_and_find_max_clique_kmn_iconditions(kmn_key, type):
    group_kmn_iconditions(kmn_key, type)
    print("find max clique ...")
    iscu.find_max_clique_2(*kmn_data[kmn_key], type)


def find_max_clique(kmn_key, type):
    print("find max clique ...")
    iscu.find_max_clique_3(*kmn_data[kmn_key], type)


if __name__ == '__main__':
    # check_isc_data("0-2-1")
    # check_isc_data("1-1-0")
    # check_011_programs()
    # check_011_icondition()
    # check_110_icondition()
    # check_021_icondition()
    # check_isc_data("1-2-0", False)
    # check_120_icondition_no_292()
    # check_120_icondition_with_292()
    # check_isc_data("1-1-1", False)
    # check_111_icondition_3_3()
    # group_111_iconditions("s")
    # group_kmn_iconditions("0-2-1", "")
    # find_021_max_clique()
    # group_and_find_max_clique_kmn_iconditions("1-2-0", "")
    # group_and_find_max_clique_kmn_iconditions("0-2-1", "")
    # group_and_find_max_clique_kmn_iconditions("1-1-0", "")
    # group_and_find_max_clique_kmn_iconditions("0-1-1", "")
    # iscu.compute_common_isets(*kmn_data["1-2-0"], "")
    # process_010_iconditions()
    # group_and_find_max_clique_kmn_iconditions("0-1-0", "ne")
    # group_and_find_max_clique_kmn_iconditions("1-1-1", "s")

    find_max_clique("1-2-0", "")
    # find_max_clique("0-2-1", "")
    # find_max_clique("0-1-0", "")
    # find_max_clique("0-1-1", "")
    # find_max_clique("1-1-0", "")
    # find_max_clique("1-1-1", "s")


    pass
    