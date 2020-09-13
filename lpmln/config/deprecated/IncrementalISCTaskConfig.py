
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-29 15:22
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IncrementalISCTaskConfig.py
"""

import json
import lpmln.config.GlobalConfig as cfg
import os
import copy
import datetime
import lpmln.iset.OptimizationISetsUtils as oisu
from lpmln.config.deprecated.ISCTaskConfig import ISCTaskConfig
from scipy.special import comb
from lpmln.iset.ISetCondition import ISetCondition

config = cfg.load_configuration()


class IncrementalISCTask:
    def __init__(self, min_ne, max_ne, kmn, is_use_extended_rules, lp_type="lpmln"):
        self.min_ne = min_ne
        self.max_ne = max_ne
        self.k_m_n = kmn
        self.is_use_extended_rules = is_use_extended_rules
        self.rule_number = 0

        if self.is_use_extended_rules:
            self.rule_set_size = 4
        else:
            self.rule_set_size = 3

        self.lp_type = lp_type
        self.incremental_iconditions = dict()

        self.task_type = "%s-%s" % ("elp" if self.is_use_extended_rules else "dlp", str(self.lp_type))
        self.task_flag = "**%d-%d-%d (%d ~ %d) %s isc tasks**"

        self.empty_iset_ids = set()
        self.iset_number = 0

        self.unknown_iset_number = 0
        self.unknown_iset_ids = set()
        self.result_file = ""

        self.task_start_time = 0
        self.task_end_time = 0
        self.icondition_number = 0

        self.task_total_number = 0
        self.task_max_slice_size = 100
        self.task_space_size = 0

        # runtime records

        self.incremental_task_number = dict()
        self.incremental_task_complete_number = dict()
        self.incremental_task_space_size = dict()

        self.task_complete_number = 0
        self.task_progress_rate = 0.0
        self.is_find_new_se_condition = False
        self.se_condition_dump_file = "no file"

        self.complete_params()

    def complete_params(self):
        self.rule_number = sum(self.k_m_n)
        self.iset_number = 2 ** (self.rule_set_size * self.rule_number) - 1
        self.empty_iset_ids = oisu.get_empty_indpendent_set_ids(self.rule_number, self.is_use_extended_rules)
        self.unknown_iset_number = self.iset_number - len(self.empty_iset_ids)

        for i in range(self.iset_number):
            if i not in self.empty_iset_ids:
                self.unknown_iset_ids.add(i)

        for i in range(1, self.max_ne + 1):
            self.incremental_iconditions[i] = list()
            self.incremental_task_number[i] = 0
            self.incremental_task_space_size[i] = comb(self.unknown_iset_number, i)
            self.incremental_task_complete_number[i] = 0
            self.task_space_size += self.incremental_task_space_size[i]

        self.result_file = config.get_isc_results_file_path(self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne, self.task_type)
        self.task_flag = self.task_flag % (self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne, self.task_type)

    def generate_isc_task_items_by_base_icondition(self, base_icondition):
        isc_task_items = list()
        task_batch =set()
        ne_iset_ids = base_icondition.ne_iset_ids


        for iset in self.unknown_iset_ids:
            if iset not in ne_iset_ids:
                task_batch.add(iset)

            if len(task_batch) == self.task_max_slice_size:
                isc_task_items.append((copy.deepcopy(ne_iset_ids), task_batch))
                task_batch = set()

        if len(task_batch) != 0:
            isc_task_items.append((copy.deepcopy(ne_iset_ids), task_batch))

        task_items_size = len(self.unknown_iset_ids) - len(ne_iset_ids)
        self.incremental_task_number[len(ne_iset_ids) + 1] += task_items_size
        self.task_total_number += task_items_size

        msg_text = "generate %d %s from base icondition %s" % (task_items_size, self.task_flag, base_icondition.get_ne_iset_str())

        return isc_task_items, msg_text

    def get_initial_isc_task_items(self):
        tmp = self.task_max_slice_size
        self.task_max_slice_size = 2
        icondition = ISetCondition(list(), list())
        itask_items, msg_text = self.generate_isc_task_items_by_base_icondition(icondition)
        self.task_max_slice_size = tmp
        return itask_items, msg_text

    def insert_se_condition(self, icondition):
        self.icondition_number += 1
        base_ne_iset_number = icondition.ne_iset_number
        self.incremental_iconditions[base_ne_iset_number].append(icondition)
        if base_ne_iset_number < self.max_ne:
            next_isc_task_items, msg_text = self.generate_isc_task_items_by_base_icondition(icondition)
        else:
            next_isc_task_items = list()
            msg_text = "no more new %s items from base icondition %s!" % (self.task_flag, icondition.get_ne_iset_str())

        self.is_find_new_se_condition = True

        return next_isc_task_items, msg_text

    def dump_tmp_se_condition(self):
        if self.is_find_new_se_condition:
            self.is_find_new_se_condition = False

            file_time_fmt = "%Y_%m_%d_%H_%M_%S"
            now_time = datetime.datetime.now()
            file_name = "%d-%d-%d-%s-isc-%d-%d-emp-%s.txt" % (
                self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.task_type, self.min_ne, self.max_ne, now_time.strftime(file_time_fmt))
            file_name = os.path.join(config.isc_tmp_path, file_name)
            self.se_condition_dump_file = file_name
            self.save_se_condition(file_name)

    def get_progress_info(self):
        if self.task_complete_number == 0:
            prg_info = ":timer_clock:  %s: total tasks: %d, waiting for searching!" % (self.task_flag, self.task_total_number)
        else:
            self.task_progress_rate = 100.0 * self.task_complete_number / self.task_total_number
            task_running_time = self.task_end_time - self.task_start_time
            if self.icondition_number == 0:
                prg_info = ":mag_right: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find 0 se conditions. details: \n%s" % (
                    self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate, str(task_running_time), self.get_itask_item_detail_status())
            else:
                prg_info = ":rocket: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find %d se conditions,  dumped to %s. details: \n%s" % (
                    self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate,
                    str(task_running_time), self.icondition_number, self.se_condition_dump_file, self.get_itask_item_detail_status())
        return prg_info

    def get_itask_item_detail_status(self):
        status = list()
        tmpl = "\t\t\t\t non-empty iset number: %d, space size: %d, task items progress: %d / %d (speed up: %3.f), found %d se condition"
        for i in range(1, self.max_ne + 1):
            space_size = self.incremental_task_space_size[i]
            task_number = self.incremental_task_number[i]
            speed_up_ratio = 1.0 * space_size / task_number
            st = tmpl % (i, space_size, self.incremental_task_complete_number[i],
                         task_number, speed_up_ratio, len(self.incremental_iconditions[i]))
            status.append(st)

        return "\n".join(status)

    def task_finish(self):
        self.save_se_condition(self.result_file)
        self.se_condition_dump_file = self.result_file
        return self.get_progress_info()

    def save_se_condition(self, file_name):
        with open(file_name, mode='w', encoding='utf-8') as f:
            for i in self.incremental_iconditions:
                for se in self.incremental_iconditions[i]:
                    f.write(str(se))
                    f.write("\n")


    def set_task_running_time(self, running_time):
        st = running_time[0]
        et = running_time[1]

        if self.task_start_time == 0:
            self.task_start_time = st
        else:
            if self.task_start_time.__gt__(st):
                self.task_start_time = st

        if self.task_end_time == 0:
            self.task_end_time = et
        else:
            if self.task_end_time.__lt__(et):
                self.task_end_time = et

    def set_task_complete_number(self, task_complete_number, ne_iset_number):
        self.incremental_task_complete_number[ne_iset_number] += task_complete_number
        self.task_complete_number += task_complete_number

    def has_new_itask_items(self):
        if self.task_complete_number < self.task_total_number:
            return True
        else:
            return False


class IncrementalISCTaskConfig(ISCTaskConfig):
    def load_isc_config_file(self):
        with open(self.config_file, encoding="utf-8", mode="r") as cf:
            jobj = json.load(cf)
            for task in jobj:
                if task["is_skip"]:
                    continue

                rule_number = task["rule_number"]
                max_ne = task["max_ne"]

                if "lp_type" not in task:
                    lp_type = "lpmln"
                else:
                    lp_type = task["lp_type"]

                kmns_key = "kmns"
                if kmns_key in task:
                    kmns = self.get_kmn_from_config(task[kmns_key])
                else:
                    kmns = self.get_kmn_by_rule_number(rule_number)

                for kmn in kmns:
                    itask = IncrementalISCTask(1, max_ne, kmn, self.is_use_extended_rules, lp_type)
                    self.isc_tasks.append(itask)

if __name__ == '__main__':
    pass
    