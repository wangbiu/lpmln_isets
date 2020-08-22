
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-22 10:42
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SESATTableUtils.py
"""


def generate_single_deletion_empty_table():
    table = list()
    for i in range(3):
        item = list()
        for j in range(2):
            item.append(set())
        table.append(item)
    return table


def generate_0000_single_deletion_sat_result():
    table = generate_single_deletion_empty_table()
    for model in table:
        model[0].add(0)
        model[1].add(1)
    return table


def print_single_deletion_sat_result_table(all_case_flags, result_table):
    for case in all_case_flags:
        table = result_table[case]
        print(case, "|", table[0][1], "|", table[0][0])

    print("")
    for case in all_case_flags:
        table = result_table[case]
        print(case, "|", table[1][1], "|", table[1][0], "|", table[2][1], "|", table[2][0])

    print("\n")


def extract_direct_single_deletion_case_sat_results(case_sat_results, total_results):
    for case in case_sat_results:
        if case not in total_results:
            total_results[case] = generate_single_deletion_empty_table()
        case_table = total_results[case]
        sat_results = case_sat_results[case]

        for sr in sat_results:
            del_rule_sat = sr[3]
            for i in range(0, 3):
                case_table[i][sr[i]].add(del_rule_sat)


if __name__ == '__main__':
    pass
    