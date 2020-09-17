
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-17 22:05
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : CombinationSpaceUtils.py
"""

import copy
import itertools


class CombinationSearchingSpaceSplitter:
    @staticmethod
    def vandermonde_split(left_zone_elements, right_zone_elements, choice_number):
        searching_slices = list()
        left_zone_length = len(left_zone_elements)
        right_zone_length = len(right_zone_elements)
        for left_choice_number in range(choice_number + 1):
            right_choice_number = choice_number - left_choice_number
            if left_choice_number > left_zone_length or right_choice_number > right_zone_length:
                continue

            split_iter = itertools.combinations(left_zone_elements, left_choice_number)
            for lce in split_iter:
                left_choice = copy.deepcopy(lce)
                slice = (left_choice, right_zone_elements, right_choice_number)
                searching_slices.append(slice)
        return searching_slices

    @staticmethod
    def yanghui_split(all_elements, choice_number, split_elements):
        searching_slices = list()

        if choice_number < len(split_elements):
            ts = (set(), all_elements, choice_number)
            searching_slices.append(ts)
        else:
            eliminate_elements = set()
            for ele in split_elements:
                choice_elements = copy.deepcopy(eliminate_elements)
                eliminate_elements.add(ele)
                all_elements.remove(ele)
                remained_elements = copy.deepcopy(all_elements)
                remained_choice_number = choice_number - len(choice_elements)
                ts = (choice_elements, remained_elements, remained_choice_number)
                searching_slices.append(ts)

            ts = (eliminate_elements, all_elements, choice_number - len(split_elements))
            searching_slices.append(ts)

        return searching_slices


def vandermonde_split_checker(max_elements_size=10):
    elements = [i for i in range(max_elements_size)]
    left_length = max_elements_size // 3
    if left_length > 12:
        left_length = 12
    left_zone = elements[0:left_length]
    right_zone = elements[left_length:]
    for choice_number in range(max_elements_size + 1):
        total_search_number = CombinaryCounter.compute_comb(max_elements_size, choice_number)
        slices_search_number = 0
        searching_slices = CombinationSearchingSpaceSplitter.vandermonde_split(left_zone, right_zone, choice_number)
        for ts in searching_slices:
            slices_search_number += CombinaryCounter.compute_comb(len(ts[1]), ts[2])

        msg_text = "C(%d, %d), search slices number %d: real = %d, slices sum = %d, is same %s" % (max_elements_size,
            choice_number, len(searching_slices), total_search_number, slices_search_number, str(total_search_number == slices_search_number))
        print(msg_text)

        if total_search_number != slices_search_number:
            raise RuntimeError(msg_text)


def yanghui_split_checker(max_elements_size=10):
    elements = {i for i in range(max_elements_size)}
    choice_elements = {random.randint(0, max_elements_size-1) for i in range(4)}
    print(choice_elements)
    for choice_number in range(max_elements_size + 1):
        total_search_number = CombinaryCounter.compute_comb(max_elements_size, choice_number)
        slices_search_number = 0
        all_elements = copy.deepcopy(elements)
        searching_slices = CombinationSearchingSpaceSplitter.yanghui_split(all_elements, choice_number, choice_elements)
        for ts in searching_slices:
            slices_search_number += CombinaryCounter.compute_comb(len(ts[1]), ts[2])

        msg_text = "C(%d, %d), search slices number %d: real = %d, slices sum = %d, is same %s" % (
            max_elements_size, choice_number, len(searching_slices), total_search_number, slices_search_number,
            str(total_search_number == slices_search_number))
        print(msg_text)

        if total_search_number != slices_search_number:
            raise RuntimeError(msg_text)

    pass


if __name__ == '__main__':
    from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
    import random

    # vandermonde_split_checker(60)
    yanghui_split_checker(60)
    pass
    