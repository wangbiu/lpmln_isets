
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-24 21:08
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IConditionsSimplifier.py
"""

from sympy import symbols, simplify, true, false
from sympy.logic.boolalg import And, Or, Not, to_dnf
import lpmln.iset.ISetUtils as isu


def get_dnf_from_iconditions(file, ignore_isets):
    iconditions = isu.load_iconditions_from_file(file)
    iconditions = isu.parse_iconditions_ignore_singletons(iconditions)
    iset_size = len(iconditions[0])
    chars = ["a" + str(i) for i in range(iset_size)]
    symbol_chars = " ".join(chars)
    syms = symbols(symbol_chars)
    print(chars)
    left = false
    right = false
    for ic in iconditions:
        right = get_conjunction_formula(ic, syms, ignore_isets)
        left = Or(left, right)
    return left


def get_conjunction_formula(isp_cdt, syms, ignore_isets):
    left = true
    for i in range(len(syms)):
        if i not in ignore_isets:
            if isp_cdt[i] == 0:
                right = Not(syms[i])
            else:
                right = syms[i]
            left = And(left, right)

    return left


def isp_simplify(file, ignore_isets=set()):
    formula = get_dnf_from_iconditions(file, ignore_isets)
    print(formula)
    simplified = simplify(formula)
    print("simplified: ", simplified)
    # print(to_dnf(left, simplify=True))
    # msg.send_message(str(simplified))
    return formula


def check_01_distributions_of_iconditions(file):
    iconditions = isu.load_iconditions_from_file(file)
    iconditions = isu.parse_iconditions_ignore_singletons(iconditions)
    iset_size = len(iconditions[0])
    zero_cnts = [0] * iset_size
    one_cnts = [0] * iset_size

    for col in range(iset_size):
        zero_c = 0
        one_c = 0
        for ic in iconditions:
            if ic[col] == 0:
                zero_c += 1
            elif ic[col] == 1:
                one_c += 1
        zero_cnts[col] = zero_c
        one_cnts[col] = one_c

    for col in range(iset_size):
        print("col-%d: zero = %d, one = %d, sum = %d" % (col, zero_cnts[col], one_cnts[col], zero_cnts[col] + one_cnts[col]))

    all_zero_cols = []
    all_ones_cols = []
    one_zero_eq_cols = []
    for col in range(iset_size):
        zero = zero_cnts[col]
        one = one_cnts[col]

        if one == 0:
            print("col-%d all zeros" % col)
            all_zero_cols.append(col)
        elif zero == 0:
            print("col-%d all ones" % col)
            all_ones_cols.append(col)
        elif one == zero:
            one_zero_eq_cols.append(col)

    print("all zeros cols ", all_zero_cols)
    print("all ones cols ", all_ones_cols)
    print("one zero eq cols", one_zero_eq_cols)


def analysis_iconditions_by_nonempty_isets_numbers(icondition_file, min_ne_iset_number, max_ne_iset_number):
    conditions = isu.load_iconditions_from_file(icondition_file)
    conditions = isu.parse_iconditions_ignore_singletons(conditions)
    ne_iset_conditions = dict()
    ne_iset_ids = dict()
    for i in range(min_ne_iset_number, max_ne_iset_number + 1):
        nc = list()
        ne_iset_conditions[i] = nc
        nc = list()
        ne_iset_ids[i] = nc

    for ic in conditions:
        number = isu.get_ne_iset_number(ic)
        ne_iset_conditions[number].append(ic)
        ids = isu.get_ne_iset_ids(ic)
        ne_iset_ids[number].append(ids)

    chains = list()
    for i in range(min_ne_iset_number + 1, max_ne_iset_number + 1):
        for ids in ne_iset_ids[i]:
            chain = find_parent_icondition(ne_iset_ids[i-1], ids)
            if chain == -1:
                print("unchained iconditions non empty isets", ids)
            else:
                chains.append(chain)
                print(chain)

    for i in ne_iset_conditions:
        print("non empty isets number %d, has %d iconditions, iset ids: " % (i, len(ne_iset_conditions[i])), ne_iset_ids[i])


def find_parent_icondition(parent_ne_iset_ids, iset_ids):
    for i in range(len(parent_ne_iset_ids)):
        ids = parent_ne_iset_ids[i]
        if ids.issubset(iset_ids):
            chain = generate_set_chain(ids, iset_ids)
            return chain
    return -1

def generate_set_chain(s1, s2):
    s1 = [str(s) for s in s1]
    s2 = [str(s) for s in s2]
    chain = "{%s} -> {%s}" % (",".join(s1), ",".join(s2))
    return chain




if __name__ == '__main__':
    file_010_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc-non-se.txt"
    file_010_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc.txt"
    file_010_nse2 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-non-se-3sets.txt"
    file_010_asp3_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-asp-non-se-3sets.txt"
    file_010_asp4_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc-asp-nse.txt"
    file_011_lpmln4_1_6 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-1-6-emp.txt"
    file_011_lpmln4_7_7 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-7-7-emp.txt"
    file_011_asp4_1_6 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-1-6-asp-emp.txt"
    file_011_asp4_7_7 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-7-7-asp-emp.txt"

    ignore_isets = {2, 5, 6, 10, 11, 12, 13, 14}
    # isp_simplify(file_010_nse, ignore_isets)

    # check_01_distributions_of_iconditions(file_010_nse)
    # check_01_distributions_of_iconditions(file_010_asp4_nse)
    # isp_simplify(file_010_asp4_nse, ignore_isets)

    # check_01_distributions_of_iconditions(file_010_se)
    analysis_iconditions_by_nonempty_isets_numbers(file_011_lpmln4_1_6, min_ne_iset_number=1, max_ne_iset_number=6)
    # analysis_iconditions_by_nonempty_isets_numbers(file_011_asp4_7_7, min_ne_iset_number=7, max_ne_iset_number=7)
    pass
    