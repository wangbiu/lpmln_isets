
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-09 3:53
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ParallelCombination.py
"""

from scipy.special import comb
import copy
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter


def combination(n, m, k):
    """
    范德蒙恒等式实现例 1
    """
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
    """
    范德蒙恒等式实现例 2
    """
    # answer = comb(unknown_iset_number, ne_iset_number)
    answer = compute_comb(unknown_iset_number, ne_iset_number)
    compute = 0
    left_length = int(unknown_iset_number / 2)
    if left_length > 12:
        left_length = 12
    # k = 10

    right_length = unknown_iset_number - left_length

    # print("C(%d, %d) = " % (unknown_iset_number, ne_iset_number))

    # if ne_iset_number > left_length:
    #     left_length = right_length
    #
    # right_length = unknown_iset_number - left_length


    for left_iset_number in range(ne_iset_number + 1):
        right_iset_number = ne_iset_number - left_iset_number
        if left_iset_number > left_length or right_iset_number > right_length:
            continue

        formula = "C(%d, %d) * C(%d, %d)" % (left_length, left_iset_number, right_length, right_iset_number)
        tmp = compute_comb(left_length, left_iset_number) * compute_comb(right_length, right_iset_number)
        compute += tmp

        # print("\t", formula)

    if answer != compute:
        msg = "total iset %d, ne iset %d, real value %d, computed value %d, same %s" % (unknown_iset_number, ne_iset_number, answer, compute, answer == compute)
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


def yanghui_triangle_number(n, m):
    real = compute_comb(n, m)
    part1 = compute_comb(n-1, m)
    part2 = compute_comb(n-1, m-1)
    add = part2 + part1
    print("C(%d, %d) = C(%d, %d) + C(%d, %d) \n\t = %d + %d = %d \n\t %s" % (n, m, n-1, m, n-1, m-1, part1, part2, add, str(add == real)))
    if real != add:
        raise RuntimeError("compute error: C(%d, %d)" % (n, m))


def yanghui_triangle_number_sets(nse_isets, original_left_isets, all_isets, pick_number):
    skip_number = 0
    task_slices = list()
    remain_nse_isets = nse_isets.difference(original_left_isets)
    if len(remain_nse_isets) == 0:
        skip_number = CombinaryCounter.compute_comb(len(all_isets), pick_number)
        return skip_number, task_slices

    if remain_nse_isets.issubset(all_isets) and len(remain_nse_isets) <= pick_number:
        nse_size = len(remain_nse_isets)
        remain_nse_isets = list(remain_nse_isets)
        eliminate_atoms = set()
        right_zone_isets = copy.deepcopy(all_isets)
        for i in range(nse_size + 1):
            if i == nse_size:
                skip_number = CombinaryCounter.compute_comb(len(right_zone_isets), pick_number - nse_size)
            else:
                left_isets = copy.deepcopy(eliminate_atoms)
                eliminate_atoms.add(remain_nse_isets[i])
                right_zone_isets.remove(remain_nse_isets[i])
                right_isets_number = pick_number - len(left_isets)
                left_isets = left_isets.union(original_left_isets)
                task_item = (left_isets, copy.deepcopy(right_zone_isets), right_isets_number)
                task_slices.append(task_item)
    else:
        task_item = (original_left_isets, all_isets, pick_number)
        task_slices.append(task_item)

    real = CombinaryCounter.compute_comb(len(all_isets), pick_number)
    compute = skip_number
    for ti in task_slices:
        compute += CombinaryCounter.compute_comb(len(ti[1]), ti[2])

    # print(real, compute, compute == real)
    #
    # print("skip number ", skip_number)
    # print("compute tasks: ")
    # for ti in task_slices:
    #     print(ti)
    #
    # if compute != real:
    #     raise RuntimeError("wrong case: ", pick_number)

    return skip_number, task_slices


def yanghui_triangle_number_sets_2(minmal_i4_isets_tuples, left_iset_ids, right_zone_isets, right_iset_number):
    left_iset_ids = set(left_iset_ids)
    right_zone_isets = set(right_zone_isets)

    task_slices = [(left_iset_ids, right_zone_isets, right_iset_number)]
    skip_task_number = 0
    cnt = 0
    for nse in minmal_i4_isets_tuples:
        nse_new_task_slices = list()
        for ts in task_slices:
            ts_skip_task_number, new_task_slices = yanghui_triangle_number_sets(nse, *ts)
            skip_task_number += ts_skip_task_number
            nse_new_task_slices.extend(new_task_slices)
        task_slices = nse_new_task_slices

        cnt += 1
        print("nse %d: " % cnt, nse)
        for ts in task_slices:
            print("\t", ts)
        print("\n")


    real = CombinaryCounter.compute_comb(len(right_zone_isets), right_iset_number)
    compute = skip_task_number
    for ti in task_slices:
        compute += CombinaryCounter.compute_comb(len(ti[1]), ti[2])

    print(real, compute, compute == real)

    print("skip number ", skip_task_number)
    print("compute tasks: ")
    for ti in task_slices:
        print(ti)

    if compute != real:
        raise RuntimeError("wrong case: ", pick_number)

    return skip_task_number, task_slices


if __name__ == '__main__':
    iset_number = 11
    # for iset_number in range(33, 55):
    #     for i in range(1, iset_number):
    #         combination2(iset_number, i)
    # # combination2(iset_number, 16)
    # for i in range(1, iset_number + 1):
    #     yanghui_triangle_number(iset_number, i)
    # pass

    all_isets = set([i for i in range(iset_number)])
    nse_isets = {3, 5, 1, 10}
    pick_number = 4
    left_isets = {-1, -2}

    nse_conditions = [
        {1, 3},
        {2, 5},
        {1, 2, 4}
    ]

    nse_011 = [
        [0, 35],
        [1, 35],
        [35, 4],
        [35, 7],
        [9, 35],
        [35, 39],
        [41, 35],
        [35, 15],
        [16, 35],
        [35, 20],
    ]

    # for i in range(iset_number - 1, iset_number):
    #     yanghui_triangle_number_sets(nse_isets, left_isets, all_isets, i)

    iset_number = 16
    for i in range(iset_number):
        yanghui_triangle_number_sets_2(nse_conditions, left_isets, all_isets, pick_number)

    