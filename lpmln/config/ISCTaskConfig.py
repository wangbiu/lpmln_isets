# -*- encoding: utf-8 -*-
"""
@Time    : 2020/5/22 15:27
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISCTaskConfig.py
"""

import json
import lpmln.config.GlobalConfig as cfg
import os
import copy
import datetime
import lpmln.iset.OptimizationISetsUtils as oisu

config = cfg.load_configuration()


class ISCTask:
    def __init__(self, rule_number, min_ne, max_ne, kmn):
        self.rule_number = rule_number
        self.min_ne = min_ne
        self.max_ne = max_ne

        self.empty_iset_ids = set()
        self.iset_number = 0
        self.task_slice_file = ""
        self.unknown_iset_number = 0
        self.result_file = ""
        self.task_flag = "**%d-%d-%d (%d ~ %d) isc tasks**"
        self.k_m_n = kmn
        self.task_start_time = 0
        self.task_end_time = 0

        self.complete_params()
        self.task_total_number = 0
        self.task_slice_number = 0

        # runtime records
        self.task_complete_number = 0
        self.task_progress_rate = 0.0
        self.se_conditions = list()
        self.is_find_new_se_condition = False
        self.se_condition_dump_file = "no file"

    def complete_params(self):
        self.iset_number = 2 ** (3 * self.rule_number) - 1
        self.task_slice_file = config.get_task_slice_file_path(self.rule_number, self.min_ne, self.max_ne)
        self.empty_iset_ids = oisu.get_empty_indpendent_set_ids(self.rule_number)
        self.unknown_iset_number = self.iset_number - len(self.empty_iset_ids)
        self.result_file = config.get_isc_results_file_path(self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne)
        self.task_flag = self.task_flag % (self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne)

    def load_isc_task_items(self):
        isc_task_items = list()

        with open(self.task_slice_file, mode="r", encoding="utf-8") as itk:
            for ts in itk:
                task_slice_data = ts.split(",")
                slice_from = ",".join(task_slice_data[0:-1])
                slice_length = int(task_slice_data[-1])
                isc_task_items.append((slice_from, slice_length))
                self.task_slice_number += 1
                self.task_total_number += slice_length

        return isc_task_items

    def get_isc_task_load_message(self):
        msg_text = "load %s, put %d isc task slices" % (self.task_flag, self.task_slice_number)
        return msg_text

    def insert_se_condition(self, condition):
        self.se_conditions.append(condition)
        self.is_find_new_se_condition = True

    def dump_tmp_se_condition(self):
        if self.is_find_new_se_condition:
            self.is_find_new_se_condition = False
            if self.task_complete_number == self.task_total_number:
                self.task_finish()
            else:
                file_time_fmt = "%Y_%m_%d_%H_%M_%S"
                now_time = datetime.datetime.now()
                file_name = "%d-%d-%d-isc-%d-%d-emp-%s.txt" % (
                    self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne, now_time.strftime(file_time_fmt))
                file_name = os.path.join(config.isc_tmp_path, file_name)
                self.se_condition_dump_file = file_name
                self.save_se_condition(file_name)

    def get_progress_info(self):
        if self.task_complete_number == 0:
            prg_info = ":unicorn: %s: total tasks: %d, waiting for resources !" % (self.task_flag, self.task_total_number)
        else:
            self.task_progress_rate = 100.0 * self.task_complete_number / self.task_total_number
            if len(self.se_conditions) == 0:
                prg_info = ":unicorn: %s: total tasks: %d, complete tasks: %d (%.3f%%), find 0 se conditions." % (
                    self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate)
            else:
                task_running_time = self.task_end_time - self.task_start_time
                prg_info = ":unicorn: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find %d se conditions,  dumped to %s" % (
                    self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate,
                    str(task_running_time), len(self.se_conditions), self.se_condition_dump_file)
        return prg_info

    def task_finish(self):
        self.save_se_condition(self.result_file)
        self.se_condition_dump_file = self.result_file
        return self.get_progress_info()

    def save_se_condition(self, file_name):
        with open(file_name, mode='w', encoding='utf-8') as f:
            for se in self.se_conditions:
                f.write(se)
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


class ISCTaskConfig:
    def __init__(self, config_file):
        self.config_file = os.path.join(config.project_base_dir, config_file)
        self.isc_tasks = list()

        with open(self.config_file, encoding="utf-8", mode="r") as cf:
            jobj = json.load(cf)
            for task in jobj:
                if task["is_skip"]:
                    continue

                rule_number = task["rule_number"]
                min_ne = task["min_ne"]
                max_ne = task["max_ne"]

                kmns_key = "kmns"
                if kmns_key in task:
                    kmns = self.get_kmn_from_config(task[kmns_key])
                else:
                    kmns = self.get_kmn_by_rule_number(rule_number)

                for kmn in kmns:
                    itask = ISCTask(rule_number, min_ne, max_ne, kmn)
                    self.isc_tasks.append(itask)

    def get_kmn_by_rule_number(self, rule_number):
        kmns = list()
        for k in range(rule_number):
            kmn = [0] * 3
            kmn[0] = k
            m_n = rule_number - k
            for m in range(m_n + 1):
                if m_n - m > m:
                    continue
                elif m == rule_number:
                    continue
                else:
                    kmn[1] = m
                    kmn[2] = m_n - m
                    kmns.append(copy.deepcopy(kmn))

        return kmns

    def get_kmn_from_config(self, kmn_strs):
        kmns = list()
        for ks in kmn_strs:
            kmn = ks.split("-")
            kmn = [int(s) for s in kmn]
            kmns.append(kmn)

        return kmns


if __name__ == '__main__':
    tsc = ISCTaskConfig("isc-tasks-temp.json")
    pass
