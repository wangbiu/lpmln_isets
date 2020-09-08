
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-09 3:53
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ParallelCombination.py
"""

from scipy.special import comb


def combination(n, m, k):
    answer = int(comb(n, m))
    compute = 0
    for i in range(m+1):
        if i > n - k or m - i > k:
            continue


        formula = "C(%d, %d) * C(%d, %d)" % (n-k, i, k, m-i)
        tmp = comb(n-k, i) * comb(k, m-i)
        compute += tmp

        print(formula)
    print(answer == compute)


def combination2(unknown_iset_number, ne_iset_number):
    answer = int(comb(unknown_iset_number, ne_iset_number))
    compute = 0
    k = int(unknown_iset_number / 2)
    if k > 12:
        k = 12

    for i in range(ne_iset_number + 1):
        if i > k or ne_iset_number - i > unknown_iset_number - k:
            continue

        formula = "C(%d, %d) * C(%d, %d)" % (k, i, unknown_iset_number - k, ne_iset_number - i)
        tmp = comb(k, i) * comb(unknown_iset_number - k, ne_iset_number - i)
        compute += tmp

        print(formula)

    print(answer == compute)




if __name__ == '__main__':
    combination2(33, 14)
    pass
    