
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
    # answer = comb(unknown_iset_number, ne_iset_number)
    answer = compute_comb(unknown_iset_number, ne_iset_number)
    compute = 0
    left_length = int(unknown_iset_number / 2)
    if left_length > 12:
        left_length = 12
    # k = 10

    right_length = unknown_iset_number - left_length

    print("C(%d, %d) = " % (unknown_iset_number, ne_iset_number))

    # if ne_iset_number > left_length:
    #     left_length = right_length
    #
    # right_length = unknown_iset_number - left_length


    for left_iset_number in range(ne_iset_number + 1):
        right_iset_number = ne_iset_number - left_iset_number
        if left_iset_number > left_length or right_iset_number > right_length:
            continue

        formula = "C(%d, %d) * C(%d, %d)" % (left_length, left_iset_number, right_length, right_iset_number)
        tmp = comb(left_length, left_iset_number) * comb(right_length, right_iset_number)
        compute += tmp

        print("\t", formula)

    if answer != compute:
        msg = "total iset %d, ne iset %d, real value %f, computed value %f, same %s" % (unknown_iset_number, ne_iset_number, answer, compute, answer == compute)
        print(msg)


def compute_comb(n, m):
    part1 = product(n-m+1, n)
    part2 = product(1, m)
    return part1 / part2


def product(i, j):
    result = 1
    for k in range(i, j + 1):
        result *= k
    return result



if __name__ == '__main__':
    iset_number = 33
    for iset_number in range(1, 50):
        for i in range(1, iset_number):
            combination2(iset_number, i)
    # combination2(iset_number, 16)
    pass
    