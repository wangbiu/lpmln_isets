
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


def get_dnf_from_iconditions(file):
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
        right = get_conjunction_formula(ic, syms)
        left = Or(left, right)

    return left


def get_conjunction_formula(isp_cdt, syms):
    left = true
    for i in range(len(syms)):
        if isp_cdt[i] == 0:
            right = Not(syms[i])
        else:
            right = syms[i]
        left = And(left, right)

    print(left)
    return left


def isp_simplify(file):
    iconditions = isu.load_iconditions_from_file(file)
    iconditions = isu.parse_iconditions(iconditions)
    iset_size = len(iconditions[0])
    chars = ["a" + str(i+1) for i in range(iset_size)]
    symbol_chars = " ".join(chars)
    syms = symbols(symbol_chars)
    print(chars)
    left = false
    right = false
    for ic in iconditions:
        right = get_conjunction_formula(ic, syms)
        left = Or(left, right)

    print(left)
    simplified = simplify(left)
    print("simplified: ", simplified)
    # print(to_dnf(left, simplify=True))
    # msg.send_message(str(simplified))
    return left


if __name__ == '__main__':
    file_010_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc-non-se.txt"
    isp_simplify(file_010_nse)
    pass
    