
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-29 14:13
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : RawConditionUtils.py
"""

import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()
import os
import lpmln.iset.RawIConditionUtils as riu


def get_kmn_raw_data_dirs(k_size, m_size, n_size):
    kmn_dir = "%d-%d-%d" % (k_size, m_size, n_size)
    base_dir = os.path.join(config.isc_raw_results_path, kmn_dir)
    files = os.listdir(base_dir)
    kmn_raw_data_dirs = list()
    for f in files:
        ab_path = os.path.join(base_dir, f)
        if os.path.isdir(ab_path):
            kmn_raw_data_dirs.append(ab_path)

    return kmn_raw_data_dirs


def get_kmn_raw_data_slices_dir(k_size, m_size, n_size):
    kmn_dir = "%d-%d-%d" % (k_size, m_size, n_size)
    return os.path.join(config.isc_raw_results_path, kmn_dir, "ht-slices")


def get_all_kmn_raw_data_files(k_size, m_size, n_size):
    data_dirs = get_kmn_raw_data_dirs(k_size, m_size, n_size)
    data_files = list()

    for d in data_dirs:
        files = os.listdir(d)
        cnt = 0
        for f in files:
            cnt += 1
            data_files.append(os.path.join(d, f))
        print("dir %s had %d raw data files" % (d, cnt))
    return data_files


def remove_conditions_contain_k_ne_isets_from_data_file(data_file, ne_isets_numbers):
    remained_data = list()
    total_data_number = 0

    with open(data_file, encoding="utf-8", mode="r") as df:
        for data in df:
            total_data_number += 1
            ne_isets = data.strip("\r\n ").split(",")
            if len(ne_isets) not in ne_isets_numbers:
                remained_data.append(data)

    removed_condition_number = total_data_number - len(remained_data)
    print("file %s has %d data items, remain %d data items, remove %d data items" % (
        data_file, total_data_number, len(remained_data), removed_condition_number))

    with open(data_file, encoding="utf-8", mode="w") as df:
        for data in remained_data:
            df.write(data)

    return removed_condition_number


def remove_all_kmn_raw_conditions_contain_k_ne_isets(k_size, m_size, n_size, ne_isets_numbers):
    data_files = get_all_kmn_raw_data_files(k_size, m_size, n_size)
    total_remove_number = 0
    for df in data_files:
        number = remove_conditions_contain_k_ne_isets_from_data_file(df, ne_isets_numbers)
        total_remove_number += number

    print("totally remove %d data items " % total_remove_number)


def count_raw_data_number_in_file(data_file):
    cnt = 0
    with open(data_file, encoding="utf-8", mode="r") as df:
        for f in df:
            cnt += 1
    return cnt


def count_raw_data_number_in_file_by_ne_iset_number(data_file):
    results = dict()
    with open(data_file, encoding="utf-8", mode="r") as df:
        for f in df:
            data = f.strip("\r\n ").split(",")
            ne = len(data)
            if ne not in results:
                results[ne] = 0
            results[ne] += 1
    return results


def count_all_kmn_raw_condition_number(k_size, m_size, n_size):
    data_files = get_all_kmn_raw_data_files(k_size, m_size, n_size)
    total_remove_number = 0
    for df in data_files:
        print("counting %s ..." % df)
        total_remove_number += count_raw_data_number_in_file(df)
    print("%d-%d-%d raw data files has %d data items" % (k_size, m_size, n_size, total_remove_number))


def count_all_kmn_raw_condition_number_by_ne_iset_number(k_size, m_size, n_size):
    data_files = get_all_kmn_raw_data_files(k_size, m_size, n_size)
    total_remove_number = 0
    results = dict()
    for df in data_files:
        print("counting %s ..." % df)
        file_results = count_raw_data_number_in_file_by_ne_iset_number(df)
        for ne in file_results:
            if ne not in results:
                results[ne] = 0
            results[ne] += file_results[ne]

    for ne in results:
        total_remove_number += results[ne]
    print("%d-%d-%d raw data files has %d data items" % (k_size, m_size, n_size, total_remove_number))
    print(results)


def merge_all_kmn_raw_conditions(k_size, m_size, n_size, lp_type, is_use_extended_rules):
    data_files = get_all_kmn_raw_data_files(k_size, m_size, n_size)
    complete_data_file = riu.get_complete_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    outf = open(complete_data_file, mode="a", encoding="utf-8")
    for df in data_files:
        print("merge %s ..." % df)
        with open(df, mode="r", encoding="utf-8") as sub_data:
            for data in sub_data:
                outf.write(data)

    outf.close()


def merge_worker_kmn_raw_conditions(k_size, m_size, n_size, lp_type, is_use_extened_rules):
    payload = config.worker_payload
    hostname = config.worker_host_name
    outf = riu.get_raw_icondition_file_path(k_size, m_size, n_size, lp_type, is_use_extened_rules, hostname)
    outf = open(outf, encoding="utf-8", mode="w")

    for i in range(1, payload + 1):
        dataf = riu.get_raw_icondition_file_path(k_size, m_size, n_size, lp_type, is_use_extened_rules, str(i))
        print("merge %s ..." % dataf)
        with open(dataf, encoding="utf-8", mode="r") as df:
            for data in df:
                outf.write(data)
    outf.close()


def merge_and_clean_worker_kmn_raw_conditions(k_size, m_size, n_size, lp_type, is_use_extened_rules, clean_ne_iset_numbers):
    payload = config.worker_payload
    hostname = config.worker_host_name
    outf = riu.get_raw_icondition_file_path(k_size, m_size, n_size, lp_type, is_use_extened_rules, hostname)
    outf = open(outf, encoding="utf-8", mode="w")
    cnt = 0
    for i in range(1, payload + 1):
        dataf = riu.get_raw_icondition_file_path(k_size, m_size, n_size, lp_type, is_use_extened_rules, str(i))
        print("merge %s ..." % dataf)
        if not os.path.exists(dataf):
            continue

        with open(dataf, encoding="utf-8", mode="r") as df:
            for data in df:
                ne_isets = data.strip("\r\n ").split(",")
                if len(ne_isets) not in clean_ne_iset_numbers:
                    cnt += 1
                    outf.write(data)
    outf.close()
    print("worker %s has %d raw conditions" % (hostname, cnt))
    return cnt


def split_kmn_raw_conditions_by_ne_iset_numbers(k_size, m_size, n_size, lp_type, is_use_extened_rules):
    data_file = riu.get_complete_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extened_rules)
    data_file = open(data_file, encoding="utf-8", mode="r")
    data_dir = riu.get_raw_condition_split_data_dir(k_size, m_size, n_size)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    split_files = dict()

    for data in data_file:
        data_item = data.strip("\r\n ").split(",")
        ne = len(data_item)
        if ne not in split_files:
            outf = os.path.join(data_dir, str(ne))
            outf = open(outf, mode="w", encoding="utf-8")
            split_files[ne] = outf
        else:
            outf= split_files[ne]

        outf.write(data)

    for ne in split_files:
        split_files[ne].close()

    data_file.close()


def count_raw_condition_split_data(k_size, m_size, n_size):
    files = riu.get_raw_condition_split_data_files(k_size, m_size, n_size)
    cnt = 0
    for f in files:
        print("counting %s ..." % f)
        with open(f, encoding="utf-8", mode="r") as df:
            for data in df:
                cnt += 1

    print("has %d data " % cnt)


if __name__ == '__main__':
    kmn = (2, 1, 0)
    ne_iset_numbers = {18}
    # get_kmn_raw_data_dirs(*kmn)
    files = get_all_kmn_raw_data_files(*kmn)
    # print(files[0])
    # remove_conditions_contain_k_ne_isets_from_data_file(files[0], ne_iset_numbers)
    # remove_all_kmn_raw_conditions_contain_k_ne_isets(*kmn, ne_iset_numbers)
    # count_all_kmn_raw_condition_number(*kmn)
    # ht_slices_dir = get_kmn_raw_data_slices_dir(*kmn)
    # print(ht_slices_dir)
    merge_all_kmn_raw_conditions(*kmn)
    pass
    