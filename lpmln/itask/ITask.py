
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 14:54
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ITask.py
"""
import json
import lpmln.config.GlobalConfig as cfg
import os
import copy
import datetime
import lpmln.iset.ISetNonSEUtils as isnse
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
from lpmln.itask.ITaskMeta import ITaskMeta


config = cfg.load_configuration()


class ITask:
    def __init__(self, min_ne, max_ne, kmn, is_use_extended_rules, lp_type):
        self.lp_type = lp_type
        self.k_m_n = kmn
        self.rule_number = sum(kmn)
        self.min_ne = min_ne
        self.max_ne = max_ne
        self.is_use_extended_rules = is_use_extended_rules
        if self.is_use_extended_rules:
            self.rule_set_size = 4
        else:
            self.rule_set_size = 3

        self.meta_key = "%d-%d-%d-%s-%d" % (*self.k_m_n, self.lp_type, self.rule_set_size)
        self.meta_data = ITaskMeta()

        self.iset_number = 0
        self.task_slice_file = ""
        self.task_slice_file_exist = True
        self.unknown_iset_number = 0
        self.result_file = ""
        self.result_file_write = False

        self.task_type = "%s-%s" % (str(self.lp_type), "elp" if self.is_use_extended_rules else "dlp")
        self.task_flag = "**%d-%d-%d (%d ~ %d) %s isc tasks**"

        self.task_start_time = None
        self.task_end_time = None

        self.task_total_number = 0
        self.se_condition_number = 0

        # runtime records
        self.task_complete_number = 0
        self.task_progress_rate = 0.0
        self.is_find_new_se_condition = False
        self.se_condition_dump_file = "no file"
        self.working_ne_iset_numbers = self.min_ne

        # hierarchical records
        self.hierarchical_task_number = dict()
        self.hierarchical_task_complete_number = dict()
        self.hierarchical_task_check_number = dict()
        self.hierarchical_task_valid_rule_skip_number = dict()
        self.hierarchical_se_conditions = dict()
        self.hierarchical_nse_condition_number = dict()
        self.hierarchical_new_non_se_condition_number = dict()
        self.hierarchical_running_time = dict()

        # non se conditions
        self.non_se_condition_files = list()
        self.non_se_conditions_buffer = list()

        # worker
        self.non_se_conditions = list()
        self.loaded_non_se_condition_files = set()
        self.is_task_finish = False
        self.terminate_working_ne_iset_number = 0

        self.complete_params()

    def complete_params(self):
        all_meta_data = ITaskMeta.load_itask_meta_data_from_file(config.isc_meta_data_file)
        self.meta_data = all_meta_data[self.meta_key]
        self.unknown_iset_number = len(self.meta_data.search_space_iset_ids)
        if self.max_ne == -1:
            self.max_ne = self.unknown_iset_number

        self.iset_number = 2 ** (self.rule_set_size * self.rule_number) - 1
        self.result_file = config.get_isc_results_file_path(*self.k_m_n, self.min_ne, self.max_ne)
        self.task_flag = self.task_flag % (*self.k_m_n, self.min_ne, self.max_ne, self.task_type)

        for i in range(1, self.max_ne + 1):
            self.hierarchical_task_number[i] = 0
            self.hierarchical_task_complete_number[i] = 0
            self.hierarchical_task_check_number[i] = 0
            self.hierarchical_task_valid_rule_skip_number[i] = 0
            self.hierarchical_nse_condition_number[i] = 0
            self.hierarchical_new_non_se_condition_number[i] = 0
            self.hierarchical_se_conditions[i] = list()
            self.hierarchical_running_time[i] = [None, None]

    def flush_non_se_condition(self):
        non_se_file = isnse.save_kmn_non_se_results(self.k_m_n[0], self.k_m_n[1], self.k_m_n[2],
                                                    self.working_ne_iset_numbers, self.non_se_conditions_buffer,
                                                    self.lp_type, self.is_use_extended_rules)
        self.hierarchical_new_non_se_condition_number[self.working_ne_iset_numbers] = len(self.non_se_conditions_buffer)
        for nse in self.non_se_conditions_buffer:
            self.non_se_conditions.append(nse)
        self.non_se_condition_files.append(self.working_ne_iset_numbers)
        self.non_se_conditions_buffer = list()

        return non_se_file

    def is_early_terminate(self):
        # complete_itasks = self.hierarchical_task_complete_number[current_non_empty_iset_number]
        itask_number = self.hierarchical_task_number[self.working_ne_iset_numbers]
        if itask_number == 0:
            return False
        nse_icondition_number = self.hierarchical_nse_condition_number[self.working_ne_iset_numbers]
        if nse_icondition_number == itask_number:
            self.is_task_finish = True
            return True
        else:
            return False

    def is_no_new_se_condition(self):
        task_complete_number = self.hierarchical_task_complete_number[self.working_ne_iset_numbers]
        task_number = self.hierarchical_task_number[self.working_ne_iset_numbers]
        se_condition_number = len(self.hierarchical_se_conditions[self.working_ne_iset_numbers])

        if task_complete_number == task_number and se_condition_number == 0:
            return True
        else:
            return False

    def insert_se_condition(self, condition):
        ne_iset_number = len(condition.ne_iset_ids)
        self.hierarchical_se_conditions[ne_iset_number].append(condition)
        self.is_find_new_se_condition = True
        self.se_condition_number += 1

    def insert_nse_condition(self, condition):
        ne_iset_ids = condition.ne_iset_ids
        self.non_se_conditions_buffer.append(ne_iset_ids)
        self.hierarchical_nse_condition_number[len(ne_iset_ids)] += 1

    def init_task_numbers(self):
        unknown_iset_number = len(self.meta_data.search_space_iset_ids)
        for i in range(self.min_ne, self.max_ne + 1):
            task_number = CombinaryCounter.compute_comb(unknown_iset_number, i)
            self.task_total_number += task_number
            self.hierarchical_task_number[i] = task_number

    def dump_tmp_se_condition(self):
        if self.is_task_finish:
            if not self.result_file_write:
                self.task_finish()
                self.result_file_write = True
        else:
            if self.is_find_new_se_condition:
                self.is_find_new_se_condition = False
                file_time_fmt = "%Y_%m_%d_%H_%M_%S"
                now_time = datetime.datetime.now()
                file_name = "%d-%d-%d-isc-%d-%d-emp-%s.txt" % (
                    self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne, now_time.strftime(file_time_fmt))
                file_name = os.path.join(config.isc_tmp_path, file_name)
                self.se_condition_dump_file = file_name
                self.save_se_condition(file_name)

    def get_final_detail_progress_info(self):
        self.task_progress_rate = 100.0 * self.task_complete_number / self.task_total_number
        task_running_time = self.get_running_time(-1)
        details = self.get_itask_detail_status()
        prg_info = ":rocket: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find %d se conditions,  dumped to %s details: \n %s" % (
            self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate,
            str(task_running_time), self.se_condition_number, self.se_condition_dump_file, details)
        return prg_info

    def get_progress_info(self):
        task_running_time = self.get_running_time(-1)
        if not self.is_task_finish:
            if self.hierarchical_task_check_number[self.min_ne] == 0:
                prg_info = ":timer_clock:  %s: total tasks: %d, waiting for resources !" % (
                self.task_flag, self.task_total_number)
            else:
                self.task_progress_rate = 100.0 * self.task_complete_number / self.task_total_number
                details = self.get_itask_detail_status(self.working_ne_iset_numbers)
                if self.se_condition_number == 0:
                    prg_info = ":mag_right: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find 0 se conditions. details: \n %s" % (
                        self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate,
                        str(task_running_time), details)
                else:
                    prg_info = ":rocket: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find %d se conditions,  dumped to %s details: \n %s" % (
                        self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate,
                        str(task_running_time), self.se_condition_number, self.se_condition_dump_file, details)
        else:
            task_check_number = 0
            for i in range(self.min_ne, self.max_ne + 1):
                task_check_number += self.hierarchical_task_check_number[i]
            prg_info = ":rocket: %s finish: total tasks: %d, check tasks: %d, complete tasks: %d (%.3f%% (c/t), running time: %s), find %d se conditions,  dumped to %s" % (
                self.task_flag, self.task_total_number, task_check_number, self.task_complete_number, self.task_progress_rate,
                str(task_running_time), self.se_condition_number, self.se_condition_dump_file)

        return prg_info

    def get_itask_detail_status(self, working_ne_iset_number=-1):
        status = list()
        tmpl = "\t\t\t\t ne iset: %d, task progress (cp/ck/sk/t): %d / %d / %d / %d (t/ck: %.3f, running time: %s), found conditions: %d se, %d nse, %d new nse."

        if working_ne_iset_number < 0:
            min_ne = self.min_ne
            max_ne = self.max_ne
        else:
            min_ne = working_ne_iset_number - 2
            max_ne = working_ne_iset_number + 1
            if min_ne < self.min_ne:
                min_ne = self.min_ne

        for i in range(min_ne, max_ne):
            task_number = self.hierarchical_task_number[i]
            check_number = self.hierarchical_task_check_number[i]
            if check_number == 0:
                speed_up_ratio = 1.0
            else:
                speed_up_ratio = 1.0 * task_number / check_number
            complete_number = self.hierarchical_task_complete_number[i]
            new_nse_number = self.hierarchical_new_non_se_condition_number[i]
            nse_number = self.hierarchical_nse_condition_number[i]
            se_number = len(self.hierarchical_se_conditions[i])
            sk_number = self.hierarchical_task_valid_rule_skip_number[i]
            task_running_time = self.get_running_time(i)
            st = tmpl % (i, complete_number, check_number, sk_number,
                         task_number, speed_up_ratio, str(task_running_time), se_number, nse_number, new_nse_number)
            status.append(st)

        return "\n".join(status)

    def task_finish(self):
        self.save_se_condition(self.result_file)
        self.se_condition_dump_file = self.result_file
        self.save_progress_info()

        return self.get_progress_info()

    def save_se_condition(self, file_name):
        with open(file_name, mode='w', encoding='utf-8') as f:
            for i in self.hierarchical_se_conditions:
                for se in self.hierarchical_se_conditions[i]:
                    f.write(str(se))
                    f.write("\n")

    def set_task_running_time(self, running_time, ne_iset_number):
        total_running_time = self.update_running_time([self.task_start_time, self.task_end_time], running_time)
        self.task_start_time = total_running_time[0]
        self.task_end_time = total_running_time[1]

        total_running_time = self.update_running_time(self.hierarchical_running_time[ne_iset_number], running_time)
        self.hierarchical_running_time[ne_iset_number] = total_running_time

    def update_running_time(self, old_time, new_time):
        updated_time = [0, 0]
        if old_time[0] is None:
            updated_time[0] = new_time[0]
        else:
            if old_time[0] > new_time[0]:
                updated_time[0] = new_time[0]
            else:
                updated_time[0] = old_time[0]

        if old_time[1] is None:
            updated_time[1] = new_time[1]
        else:
            if old_time[1] < new_time[1]:
                updated_time[1] = new_time[1]
            else:
                updated_time[1] = old_time[1]
        return updated_time

    def set_task_complete_number(self, task_complete_number, ne_iset_number):
        self.hierarchical_task_complete_number[ne_iset_number] += task_complete_number
        self.task_complete_number += task_complete_number

    def get_running_time(self, ne_iset_number):
        if ne_iset_number == -1:
            start = self.task_start_time
            end = self.task_end_time
        else:
            times = self.hierarchical_running_time[ne_iset_number]
            start = times[0]
            end = times[1]

        if start is None or end is None:
            task_running_time = 0
        else:
            task_running_time = end - start

        return task_running_time


    def save_progress_info(self):
        file_path = config.get_itask_progress_info_file(*self.k_m_n, self.min_ne, self.max_ne, self.lp_type, self.rule_set_size)
        task_running_time = self.get_running_time(-1)
        prg_info = " %s: total tasks: %d, complete tasks: %d, running time: %s, find %d se conditions." % (
            self.task_flag, self.task_total_number, self.task_complete_number, str(task_running_time), self.se_condition_number)

        data = list()
        data.append(prg_info)
        title = "ne_iset,running_time,complete,check,skip,total,tc_ratio,se,nse,new_nse"
        data.append(title)
        for i in range(self.min_ne, self.max_ne + 1):
            complete = self.hierarchical_task_complete_number[i]
            check = self.hierarchical_task_check_number[i]
            total = self.hierarchical_task_number[i]
            skip = self.hierarchical_task_valid_rule_skip_number[i]
            task_running_time = self.get_running_time(i)

            if check == 0:
                check_c = total
            else:
                check_c = check

            ratio = 1.0 * total / check_c
            se = len(self.hierarchical_se_conditions[i])
            nse = self.hierarchical_nse_condition_number[i]
            new_nse = self.hierarchical_new_non_se_condition_number[i]

            data_item = "%d,%s,%d,%d,%d,%d,%.3f,%d,%d,%d" % (i, str(task_running_time), complete, check, skip, total, ratio, se, nse, new_nse)
            data.append(data_item)

        with open(file_path, mode="w", encoding="utf-8") as f:
            for d in data:
                f.write(d)
                f.write("\n")

    def check_contain_nse_subparts(self, iset_ids):
        for nse in self.non_se_conditions:
            if nse.issubset(iset_ids):
                return True
        return False

    def check_contain_i4_isets(self, iset_ids):
        min_i4_iset_tuples = self.meta_data.minmal_i4_isets_tuples
        for i4_isets in min_i4_iset_tuples:
            if i4_isets.issubset(iset_ids):
                return True
        return False


class ITaskConfig:
    def __init__(self, config_file):
        self.config_file = os.path.join(config.project_base_dir, config_file)
        self.isc_tasks = list()
        self.is_use_extended_rules_key = "is_use_extended_rules"
        self.load_isc_config_file()

    def load_isc_config_file(self):
        with open(self.config_file, encoding="utf-8", mode="r") as cf:
            jobj = json.load(cf)
            for task in jobj:
                if task["is_skip"]:
                    continue

                is_use_extended_rules_key = "is_use_extended_rules"
                if is_use_extended_rules_key in task:
                    is_use_extended_rules = task[is_use_extended_rules_key]
                else:
                    is_use_extended_rules = False

                lp_type_key = "lp_type"
                if lp_type_key in task:
                    lp_type = task[lp_type_key]
                else:
                    lp_type = "lpmln"

                rule_number = task["rule_number"]
                min_ne = task["min_ne"]
                max_ne = task["max_ne"]

                kmns_key = "kmns"
                if kmns_key in task:
                    kmns = self.get_kmn_from_config(task[kmns_key])
                else:
                    kmns = self.get_kmn_by_rule_number(rule_number)

                for kmn in kmns:
                    itask = ITask(min_ne, max_ne, kmn, is_use_extended_rules, lp_type)
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
    tsc = ITaskConfig("isets-tasks.json", True)
    pass
