
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
    iconditions = isu.parse_iconditions(iconditions)
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
    iconditions = isu.parse_iconditions(iconditions)
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




if __name__ == '__main__':
    file_010_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc-non-se.txt"
    file_010_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc.txt"
    file_010_nse2 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-non-se-3sets.txt"

    ignore_isets = {2, 5, 6, 10, 11, 12, 13, 14}
    # isp_simplify(file_010_nse, ignore_isets)

    check_01_distributions_of_iconditions(file_010_nse)

    # check_01_distributions_of_iconditions(file_010_se)
    pass
    