
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


def count_all_kmn_raw_condition_number(k_size, m_size, n_size):
    data_files = get_all_kmn_raw_data_files(k_size, m_size, n_size)
    total_remove_number = 0
    for df in data_files:
        print("counting %d ..." % df)
        total_remove_number += count_raw_data_number_in_file(df)
    print("%d-%d-%d raw data files has %d data items" % (k_size, m_size, n_size, total_remove_number))


def merge_all_kmn_raw_conditions(k_size, m_size, n_size):
    data_files = get_all_kmn_raw_data_files(k_size, m_size, n_size)
    complete_data_file = riu.get_complete_raw_icondition_file(k_size, m_size, n_size, "lpmln", False)
    outf = open(complete_data_file, mode="a", encoding="utf-8")
    for df in data_files:
        print("merge %s ..." % df)
        with open(df, mode="r", encoding="utf-8") as sub_data:
            for data in sub_data:
                outf.write(data)

    outf.close()





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
    