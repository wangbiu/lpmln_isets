
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-29 20:27
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SearchSpaceChecker.py
"""


from lpmln.utils.counter.CombinaryCounter import CombinaryCounter


def compute_search_space_size(search_isets_size, choice_number):
    max_sub_space_size = 100000000000
    left_zone_size = 12
    right_zone_size = search_isets_size - left_zone_size
    space_size = CombinaryCounter.compute_comb(search_isets_size, choice_number)
    sub_spaces_size_sum = 0
    for i in range(choice_number + 1):
        left_choice_size = CombinaryCounter.compute_comb(left_zone_size, i)
        right_choice_size = CombinaryCounter.compute_comb(right_zone_size, choice_number - i)
        subspace_size = left_choice_size * right_choice_size
        sub_spaces_size_sum += subspace_size
        bigger = subspace_size > max_sub_space_size
        ratio = subspace_size / max_sub_space_size
        print("i = %d, subspace size = %d, is bigger than max size: %s, %.3f" % (i, subspace_size, str(bigger), ratio))
        if ratio > 100:
            right_bigger = right_choice_size > max_sub_space_size
            right_ratio = right_choice_size / max_sub_space_size
            print("\t right choice size %d, is bigger than max size: %s, %.3f" % (right_choice_size, right_bigger, right_ratio))

    if space_size != sub_spaces_size_sum:
        print("wrong case!")


if __name__ == '__main__':
    search_isets_size = 54
    choice_number = 18
    for choice_number in range(18, 30):
        print("choice number ", choice_number)
        compute_search_space_size(search_isets_size, choice_number)
        print("\n")
    pass
    