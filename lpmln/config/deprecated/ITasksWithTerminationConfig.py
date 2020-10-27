
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-07 1:19
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ITasksWithTerminationConfig.py
"""


import json
import lpmln.config.GlobalConfig as cfg
import os
import copy
import datetime
import lpmln.config.deprecated.ISCTasksMetaData as iscmeta
import lpmln.iset.ISetNonSEUtils as isnse
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
import lpmln.search.misc.ISCSearchingSlicesGenerator as isg


config = cfg.load_configuration()


class ISCTask:
    def __init__(self, min_ne, max_ne, kmn, is_use_extended_rules, lp_type):
        self.lp_type = lp_type
        self.k_m_n = kmn
        self.rule_number = sum(kmn)
        self.min_ne = min_ne
        self.max_ne = max_ne
        self.is_use_extended_rules = is_use_extended_rules
        self.meta_data = iscmeta.get_kmn_isc_meta_data(*kmn)
        if self.is_use_extended_rules:
            self.rule_set_size = 4
        else:
            self.rule_set_size = 3

        self.iset_number = 0
        self.task_slice_file = ""
        self.task_slice_file_exist = True
        self.unknown_iset_number = 0
        self.result_file = ""

        self.task_type = "%s-%s" % ("elp" if self.is_use_extended_rules else "dlp", str(self.lp_type))
        self.task_flag = "**%d-%d-%d (%d ~ %d) %s isc tasks**"

        self.min_i4_iset_tuples = list()


        self.task_start_time = 0
        self.task_end_time = 0

        self.task_total_number = 0
        self.task_slice_number = 0
        self.se_condition_number = 0

        # runtime records
        self.task_complete_number = 0
        self.task_progress_rate = 0.0
        self.is_find_new_se_condition = False
        self.se_condition_dump_file = "no file"
        self.working_ne_iset_numbers = self.min_ne

        # incremental records
        self.incremental_task_number = dict()
        self.incremental_task_complete_number = dict()
        self.incremental_se_conditions = dict()
        self.incremental_nse_condition_number = dict()
        self.incremental_task_check_number = dict()
        self.incremental_task_slices = dict()
        self.incremental_new_non_se_condition_number = dict()

        # non se conditions
        self.non_se_condition_files = list()
        self.non_se_conditions_buffer = list()

        # worker
        self.non_se_conditions = list()
        self.loaded_non_se_condition_files = set()
        self.is_task_finish = False
        self.complete_params()

    def complete_params(self):
        self.iset_number = 2 ** (self.rule_set_size * self.rule_number) - 1
        self.task_slice_file = config.get_task_slice_file_path_by_kmn(*self.k_m_n, self.min_ne, self.max_ne)
        self.unknown_iset_number = len(self.meta_data.se_iset_ids)
        self.result_file = config.get_isc_results_file_path(self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne)
        self.task_flag = self.task_flag % (self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne, self.task_type)

        for i in range(1, self.max_ne + 1):
            self.incremental_task_number[i] = 0
            self.incremental_task_complete_number[i] = 0
            self.incremental_task_check_number[i] = 0
            self.incremental_nse_condition_number[i] = 0
            self.incremental_new_non_se_condition_number[i] = 0
            self.incremental_se_conditions[i] = list()
            self.incremental_task_slices[i] = list()

        self.load_min_i4_iset_tuples()

    def flush_non_se_condition(self):
        non_se_file = isnse.save_kmn_non_se_results(self.k_m_n[0], self.k_m_n[1], self.k_m_n[2],
                                                    self.working_ne_iset_numbers, self.non_se_conditions_buffer,
                                                    self.lp_type, self.is_use_extended_rules)
        self.incremental_new_non_se_condition_number[self.working_ne_iset_numbers] = len(self.non_se_conditions_buffer)
        for nse in self.non_se_conditions_buffer:
            self.non_se_conditions.append(nse)
        self.non_se_condition_files.append(self.working_ne_iset_numbers)
        self.non_se_conditions_buffer.clear()

        return non_se_file

    def is_early_terminate(self):
        # complete_itasks = self.incremental_task_complete_number[current_non_empty_iset_number]
        itask_number = self.incremental_task_number[self.working_ne_iset_numbers]
        if itask_number == 0:
            return False
        nse_icondition_number = self.incremental_nse_condition_number[self.working_ne_iset_numbers]
        if nse_icondition_number == itask_number:
            self.is_task_finish = True
            return True
        else:
            return False

    def load_isc_task_items(self):
        if os.path.exists(self.task_slice_file):
            self.task_slice_file_exist = True
            with open(self.task_slice_file, mode="r", encoding="utf-8") as itk:
                for ts in itk:
                    task_slice_data = ts.split(",")
                    ne_iset_number = len(task_slice_data) - 1
                    slice_from = [int(d) for d in task_slice_data[0:-1]]
                    slice_length = int(task_slice_data[-1])
                    self.incremental_task_slices[ne_iset_number].append((slice_from, slice_length))
                    self.incremental_task_number[ne_iset_number] += slice_length
                    self.incremental_task_check_number[ne_iset_number] += slice_length
                    self.task_slice_number += 1
                    self.task_total_number += slice_length
        else:
            self.task_slice_file_exist = False
            unknown_iset_number = len(self.meta_data.se_iset_ids)
            task_queue, task_number, task_slice_number = isg.generate_isp_slices_task_queue(1000, 1, 2, unknown_iset_number)
            for ts in task_queue:
                ne_iset_number = len(ts[0])
                self.incremental_task_slices[ne_iset_number].append(ts)
                self.incremental_task_number[ne_iset_number] += ts[1]
                self.incremental_task_check_number[ne_iset_number] += ts[1]
                self.task_slice_number += 1
                self.task_total_number += ts[1]

    def load_min_i4_iset_tuples(self):
        data_file = config.isc_meta_data_file
        with open(data_file, mode="r", encoding="utf-8") as f:
            data = json.load(f)
            kmn_key = "%d-%d-%d" % tuple(self.k_m_n)
            i4_iset_key = "i4-tuples"
            tuples = data[kmn_key][i4_iset_key]
            tuples = [set(s) for s in tuples]
            self.min_i4_iset_tuples = tuples



    def get_check_itasks_by_non_empty_iset_number_from_loaded_isc_slices(self):
        ne_iset_number = self.working_ne_iset_numbers
        task_slices = self.incremental_task_slices[ne_iset_number]
        unknown_iset_number = len(self.meta_data.se_iset_ids)
        se_iset_ids = self.meta_data.se_iset_ids


        filtered_task_slices = list()
        check_icondition_number = 0

        for ts in task_slices:
            task_counter = CombinaryCounter(ne_iset_number, unknown_iset_number)
            task_counter.reset_current_indicator(ts[0])
            task_begin = copy.deepcopy(ts[0])
            task_length = 0

            for i in range(ts[1]):
                task_idx = task_counter.get_current_indicator()
                if task_length == 0:
                    task_begin = copy.deepcopy(task_idx)

                iset_ids = set()
                for t in task_idx:
                    iset_ids.add(se_iset_ids[t])

                if not self.is_contain_non_se_condition(iset_ids):
                    task_length += 1
                else:
                    if task_length > 0:
                        filtered_task_slices.append((task_begin, task_length))
                        check_icondition_number += task_length
                        task_length = 0
                    # task_idx = task_counter.get_current_indicator()
                    # task_begin = copy.deepcopy(task_idx)
            if task_length > 0:
                filtered_task_slices.append((task_begin, task_length))
                check_icondition_number += task_length

            nse_icondition_number = self.incremental_task_number[ne_iset_number] - check_icondition_number
            self.incremental_task_check_number[ne_iset_number] = check_icondition_number
            self.incremental_task_complete_number[ne_iset_number] = nse_icondition_number
            self.incremental_nse_condition_number[ne_iset_number] = nse_icondition_number
        msg_text = "task queue put %d itasks" % len(filtered_task_slices)
        return filtered_task_slices, msg_text

    def get_check_itasks_by_non_empty_iset_number_from_autogen(self):
        se_iset_ids = self.meta_data.se_iset_ids
        unknown_iset_number = len(se_iset_ids)
        ne_iset_number = self.working_ne_iset_numbers
        max_slice_size = 1000
        task_counter = CombinaryCounter(ne_iset_number, unknown_iset_number)

        filtered_task_slices = list()
        check_icondition_number = 0

        task_begin = []
        task_length = 0
        task_total_number = 0

        while True:
            task_idx = task_counter.get_current_indicator()
            if task_idx is None:
                break

            task_total_number += 1
            if task_length == 0:
                task_begin = copy.deepcopy(task_idx)

            iset_ids = set()
            for t in task_idx:
                iset_ids.add(se_iset_ids[t])

            if not self.is_contain_non_se_condition(iset_ids):
                task_length += 1
                if task_length == max_slice_size:
                    filtered_task_slices.append((task_begin, task_length))
                    check_icondition_number += task_length
                    task_length = 0
            else:
                if task_length > 0:
                    filtered_task_slices.append((task_begin, task_length))
                    check_icondition_number += task_length
                    task_length = 0

        if task_length > 0:
            filtered_task_slices.append((task_begin, task_length))
            check_icondition_number += task_length

        self.incremental_task_number[ne_iset_number] = task_total_number
        self.task_total_number += task_total_number

        nse_icondition_number = task_total_number - check_icondition_number
        self.incremental_task_check_number[ne_iset_number] = check_icondition_number
        self.incremental_task_complete_number[ne_iset_number] = nse_icondition_number
        self.incremental_nse_condition_number[ne_iset_number] = nse_icondition_number
        msg_text = "task queue put %d itasks" % len(filtered_task_slices)
        return filtered_task_slices, msg_text

    def get_check_itasks_by_non_empty_iset_number(self):
        self.working_ne_iset_numbers += 1
        ne_iset_number = self.working_ne_iset_numbers

        if ne_iset_number < 3:
            task_slices = self.incremental_task_slices[ne_iset_number]
            msg_text = "task queue put %d itasks" % len(task_slices)
            return task_slices, msg_text

        if ne_iset_number > self.max_ne:
            return list(), "task finish!"

        if self.task_slice_file_exist:
            return self.get_check_itasks_by_non_empty_iset_number_from_loaded_isc_slices()
        else:
            return self.get_check_itasks_by_non_empty_iset_number_from_autogen()


    def is_contain_non_se_condition(self, ne_iset_ids):
        for ne in self.non_se_conditions:
            if ne.issubset(ne_iset_ids):
                return True
        return False


    def get_isc_task_load_message(self):
        msg_text = "load %s, has %d isc task slices" % (self.task_flag, self.task_slice_number)
        return msg_text

    def insert_se_condition(self, condition):
        ne_iset_number = len(condition.ne_iset_ids)
        self.incremental_se_conditions[ne_iset_number].append(condition)
        self.is_find_new_se_condition = True
        self.se_condition_number += 1

    def insert_nse_condition(self, condition):
        ne_iset_ids = condition.ne_iset_ids
        self.non_se_conditions_buffer.append(ne_iset_ids)
        self.incremental_nse_condition_number[len(ne_iset_ids)] += 1

    def init_task_numbers(self):
        unknown_iset_number = len(self.meta_data.se_iset_ids)
        for i in range(self.min_ne, self.max_ne + 1):
            task_number = CombinaryCounter.compute_comb(unknown_iset_number, i)
            self.task_total_number += task_number
            self.incremental_task_number[i] = task_number


    def dump_tmp_se_condition(self):
        if self.is_find_new_se_condition:
            self.is_find_new_se_condition = False
            if self.is_task_finish:
                self.task_finish()
            else:
                file_time_fmt = "%Y_%m_%d_%H_%M_%S"
                now_time = datetime.datetime.now()
                file_name = "%d-%d-%d-isc-%d-%d-emp-%s.txt" % (
                    self.k_m_n[0], self.k_m_n[1], self.k_m_n[2], self.min_ne, self.max_ne, now_time.strftime(file_time_fmt))
                file_name = os.path.join(config.isc_tmp_path, file_name)
                self.se_condition_dump_file = file_name
                self.save_se_condition(file_name)

    def get_final_detail_progress_info(self):
        self.task_progress_rate = 100.0 * self.task_complete_number / self.task_total_number
        task_running_time = self.task_end_time - self.task_start_time
        details = self.get_itask_detail_status()
        prg_info = ":rocket: %s: total tasks: %d, complete tasks: %d (%.3f%%, running time: %s), find %d se conditions,  dumped to %s details: \n %s" % (
            self.task_flag, self.task_total_number, self.task_complete_number, self.task_progress_rate,
            str(task_running_time), self.se_condition_number, self.se_condition_dump_file, details)
        return prg_info

    def get_progress_info(self):
        if not self.is_task_finish:
            if self.task_complete_number == 0:
                prg_info = ":timer_clock:  %s: total tasks: %d, waiting for resources !" % (
                self.task_flag, self.task_total_number)
            else:
                self.task_progress_rate = 100.0 * self.task_complete_number / self.task_total_number
                task_running_time = self.task_end_time - self.task_start_time
                details = self.get_itask_detail_status()
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
            task_running_time = self.task_end_time - self.task_start_time
            for i in range(self.min_ne, self.max_ne + 1):
                task_check_number += self.incremental_task_check_number[i]
            prg_info = ":rocket: %s finish: total tasks: %d, check tasks: %d, complete tasks: %d (%.3f%% (c/t), running time: %s), find %d se conditions,  dumped to %s" % (
                self.task_flag, self.task_total_number, task_check_number, self.task_complete_number, self.task_progress_rate,
                str(task_running_time), self.se_condition_number, self.se_condition_dump_file)

        return prg_info

    def get_itask_detail_status(self):
        status = list()
        tmpl = "\t\t\t\t non-empty iset number: %d, task items progress (complete / check total / total): %d / %d / %d (speed up: %.3f), found %d se condition, %d nse condition, %d new nse condition."
        for i in range(self.min_ne, self.max_ne + 1):
            task_number = self.incremental_task_number[i]
            check_number = self.incremental_task_check_number[i]
            if check_number == 0:
                speed_up_ratio = 1.0
            else:
                speed_up_ratio = 1.0 * task_number / check_number
            complete_number = self.incremental_task_complete_number[i]
            new_nse_number = self.incremental_new_non_se_condition_number[i]
            nse_number = self.incremental_nse_condition_number[i]
            se_number = len(self.incremental_se_conditions[i])
            st = tmpl % (i, complete_number, check_number,
                         task_number, speed_up_ratio, se_number, nse_number, new_nse_number)
            status.append(st)

        return "\n".join(status)

    def task_finish(self):
        self.save_se_condition(self.result_file)
        self.se_condition_dump_file = self.result_file
        return self.get_progress_info()

    def save_se_condition(self, file_name):
        with open(file_name, mode='w', encoding='utf-8') as f:
            for i in self.incremental_se_conditions:
                for se in self.incremental_se_conditions[i]:
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

    def load_non_se_conditions(self):
        file_prefix = ""
        base_dir = config.isc_non_se_icondition_path

    def save_progress_info(self):
        file_path = config.get_itask_progress_info_file(*self.k_m_n, self.min_ne, self.max_ne, self.lp_type, self.rule_set_size)
        task_running_time = self.task_end_time - self.task_start_time
        prg_info = " %s: total tasks: %d, complete tasks: %d, running time: %s, find %d se conditions." % (
            self.task_flag, self.task_total_number, self.task_complete_number, str(task_running_time), self.se_condition_number)

        data = list()
        data.append(prg_info)
        title = "ne_iset,complete,check,total,tc_ratio,se,nse,new_nse"
        data.append(title)
        for i in range(self.min_ne, self.max_ne + 1):
            complete = self.incremental_task_complete_number[i]
            check = self.incremental_task_check_number[i]
            total = self.incremental_task_number[i]

            if check == 0:
                check_c = total
            else:
                check_c = check

            ratio = 1.0 * total / check_c
            se = len(self.incremental_se_conditions[i])
            nse = self.incremental_nse_condition_number[i]
            new_nse = self.incremental_new_non_se_condition_number[i]

            data_item = "%d,%d,%d,%d,%.3f,%d,%d,%d" % (i, complete, check, total, ratio, se, nse, new_nse)
            data.append(data_item)

        with open(file_path, mode="w", encoding="utf-8") as f:
            for d in data:
                f.write(d)
                f.write("\n")



class ISCTaskConfig:
    def __init__(self, config_file, is_use_extended_rules):
        self.config_file = os.path.join(config.project_base_dir, config_file)
        self.isc_tasks = list()
        self.is_use_extended_rules = is_use_extended_rules
        self.load_isc_config_file()

    def load_isc_config_file(self):
        with open(self.config_file, encoding="utf-8", mode="r") as cf:
            jobj = json.load(cf)
            for task in jobj:
                if task["is_skip"]:
                    continue

                rule_number = task["rule_number"]
                min_ne = task["min_ne"]
                max_ne = task["max_ne"]

                lp_type_key = "lp_type"
                if lp_type_key in task:
                    lp_type = task[lp_type_key]
                else:
                    lp_type = "lpmln"

                kmns_key = "kmns"
                if kmns_key in task:
                    kmns = self.get_kmn_from_config(task[kmns_key])
                else:
                    kmns = self.get_kmn_by_rule_number(rule_number)

                for kmn in kmns:
                    itask = ISCTask(min_ne, max_ne, kmn, self.is_use_extended_rules, lp_type)
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
    tsc = ISCTaskConfig("isets-tasks.json", True)
    for it in tsc.isc_tasks:
        print(it.min_i4_iset_tuples)
    pass