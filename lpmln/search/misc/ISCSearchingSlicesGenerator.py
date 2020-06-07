
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/5/20 9:20
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISPSearchingSlices.py
"""

import lpmln.iset.OptimizationISetsUtils as oisu
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
import copy
import lpmln.config.GlobalConfig as cfg

config = cfg.load_configuration()


def task_slice_tuple_2_str(slice_from, slice_length):
    slice_from_str = [str(s) for s in slice_from]
    slice_from_str.append(str(slice_length))
    return ",".join(slice_from_str)


def generate_isp_slices(rule_number, max_task_slice_size=1000, min_non_empty_iset_number=1, max_non_empty_iset_number=6):
    rset_number = 3 * rule_number
    iset_number = 2 ** rset_number - 1

    if min_non_empty_iset_number < 1:
        min_non_empty_iset_number = 1

    if rule_number == 1:
        unknown_iset_number = iset_number
    else:
        empty_iset_ids = oisu.get_empty_indpendent_set_ids(rule_number)
        unknown_iset_number = iset_number - len(empty_iset_ids)

    if max_non_empty_iset_number <= 0:
        max_non_empty_iset_number = unknown_iset_number

    task_total_number = 0
    task_slice_number = 0

    task_queue = list()

    for ne_number in range(min_non_empty_iset_number, max_non_empty_iset_number + 1):
        task_counter = CombinaryCounter(ne_number, unknown_iset_number)
        task_start_idx = []
        task_idx_cnt = 0

        while True:
            task_end_idx = task_counter.get_current_indicator()
            if task_end_idx is None:
                break

            if task_idx_cnt == 0:
                task_start_idx = copy.deepcopy(task_end_idx)

            task_idx_cnt += 1

            if task_idx_cnt == max_task_slice_size:
                task_queue.append(task_slice_tuple_2_str(task_start_idx, task_idx_cnt))
                task_total_number += task_idx_cnt
                task_slice_number += 1
                task_idx_cnt = 0

        if task_idx_cnt != 0:
            task_queue.append(task_slice_tuple_2_str(task_start_idx, task_idx_cnt))
            task_total_number += task_idx_cnt
            task_slice_number += 1

    slice_file = config.get_task_slice_file_path(rule_number, min_non_empty_iset_number, max_non_empty_iset_number)
    msg_text = "rule number %d isc task %d - %d non-empty isets, has %d tasks, task batch size %d, generate %d task slices!" % (
        rule_number, min_non_empty_iset_number, max_non_empty_iset_number, task_total_number, max_task_slice_size, task_slice_number
    )
    print(msg_text)
    with open(slice_file, mode="w", encoding="utf-8") as sf:
        for ts in task_queue:
            sf.write(ts)
            sf.write("\n")


if __name__ == '__main__':
    rule_number = 1
    max_ne_iset_number = 7
    generate_isp_slices(rule_number=rule_number, max_non_empty_iset_number=max_ne_iset_number)

    pass
    