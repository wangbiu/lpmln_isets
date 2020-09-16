
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-16 20:49
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : AfterSkipNSEEarlyTPreSI4DSearch.py
"""

import copy
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
import logging
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import itertools
import lpmln.iset.ISetCompositionUtils as iscm
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.message.Messager as msg



from lpmln.search.distributed.EarlyTerminationPreSkipI4DSearch import EarlyTerminationPreSkipI4DSearchMaster, EarlyTerminationPreSkipI4DSearchWorker


class AfterSkipNSEEarlyTPreSI4DSearchMaster(EarlyTerminationPreSkipI4DSearchMaster):
    pass


class AfterSkipNSEEarlyTPreSI4DSearchWorker(EarlyTerminationPreSkipI4DSearchWorker):

    @staticmethod
    def eliminate_one_nse_condition(cls, nse_isets, original_left_isets, all_isets, pick_number):
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

        return skip_number, task_slices


    @staticmethod
    def eliminate_all_nse_conditions(cls, itask, left_iset_ids, right_zone_isets, right_iset_number):
        left_iset_ids = set(left_iset_ids)
        right_zone_isets = set(right_zone_isets)

        task_slices = [(left_iset_ids, right_zone_isets, right_iset_number)]
        skip_task_number = 0
        for nse in itask.non_se_conditions:
            nse_new_task_slices = list()
            for ts in task_slices:
                ts_skip_task_number, new_task_slices = cls.eliminate_one_nse_condition(cls, nse, *ts)
                skip_task_number += ts_skip_task_number
                nse_new_task_slices.extend(new_task_slices)
            task_slices = nse_new_task_slices

        return skip_task_number, task_slices

    @staticmethod
    def search_task_slice(cls, itask, left_iset_ids, right_zone_isets, right_iset_number, is_check_valid_rules):
        semi_valid_skip_cnt = 0
        check_cdt_cnt = 0
        task_number = 0
        nse_cdt_cnt = 0

        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)


        nse_skip_number, task_slices = \
            cls.eliminate_all_nse_conditions(cls, itask, set(left_iset_ids), right_zone_isets, right_iset_number)

        nse_cdt_cnt += nse_skip_number
        task_number += nse_skip_number

        for ts in task_slices:
            task_iter = itertools.combinations(ts[1], ts[2])
            left_iset_ids = list(ts[0])
            for right_ti in task_iter:
                non_ne_ids = list()
                non_ne_ids.extend(left_iset_ids)
                non_ne_ids.extend(list(right_ti))
                non_ne_ids = set(non_ne_ids)
                task_number += 1

                # if cls.check_contain_nse_subparts(non_ne_ids, itask):
                #     nse_cdt_cnt += 1
                #     continue

                check_cdt_cnt += 1
                is_contain_valid_rule, is_strongly_equivalent, condition = \
                    validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                        non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)

                # if not is_contain_valid_rule:
                if is_strongly_equivalent:
                    se_conditions_cache.append(condition)
                else:
                    nse_cdt_cnt += 1
                    nse_conditions_cache.append(condition)

        return nse_cdt_cnt, check_cdt_cnt, task_number, semi_valid_skip_cnt, se_conditions_cache, nse_conditions_cache


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    AfterSkipNSEEarlyTPreSI4DSearchMaster.init_kmn_isc_task_master_from_config(AfterSkipNSEEarlyTPreSI4DSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    AfterSkipNSEEarlyTPreSI4DSearchWorker.init_kmn_isc_task_workers(AfterSkipNSEEarlyTPreSI4DSearchWorker, isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    pass
    