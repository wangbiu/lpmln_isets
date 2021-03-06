
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 10:35
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : I4SetUtils.py
"""

import lpmln.config.GlobalConfig as cfg
import os

config = cfg.load_configuration()



def get_kmn_i4_result_file_by_ne_iset_number(k_size, m_size, n_size, ne_iset_number):
    file_name = "kmn-%d-%d-%d-i4-%d.txt" % (k_size, m_size, n_size, ne_iset_number)
    return os.path.join(config.isc_i4_results_path, file_name)


def get_kmn_i4_all_result_file(k_size, m_size, n_size):
    file_name = "kmn-%d-%d-%d-i4-all.txt" % (k_size, m_size, n_size)
    return os.path.join(config.isc_i4_results_path, file_name)


def split_kmn_i4_all_results_by_ne_iset_number(k_size, m_size, n_size):
    data_file = get_kmn_i4_all_result_file(k_size, m_size, n_size)
    out_file = dict()
    with open(data_file, encoding="utf-8", mode="r") as df:
        for data in df:
            data_array = data.strip("\r\n ").split(",")
            ne_iset_number = len(data_array)
            if ne_iset_number not in out_file:
                outf = get_kmn_i4_result_file_by_ne_iset_number(k_size, m_size, n_size, ne_iset_number)
                outf = open(outf, mode="w", encoding="utf-8")
                out_file[ne_iset_number] = outf
            else:
                outf = out_file[ne_iset_number]
            outf.write(data)

    for key in out_file:
        out_file[key].close()


def extract_kmn_i4_meta_data(k_size, m_size, n_size, min_ne, max_ne):
    meta = [0]
    kmn_key = "%d-%d-%d" % (k_size, m_size, n_size)
    for i in range(min_ne, max_ne + 1):
        file = get_kmn_i4_result_file_by_ne_iset_number(k_size, m_size, n_size, i)
        cnt = 0
        with open(file, mode="r", encoding="utf-8") as f:
            for data in f:
                cnt += 1
            meta.append(cnt)
    print("\"%s\":" % kmn_key, meta, ",")
    return meta



if __name__ == '__main__':
    kmns = [(0, 1, 1, 1, 1), (1, 1, 0, 1, 5), (0, 2, 1, 1, 7), (1, 2, 0, 1, 16), (1, 1, 1, 1, 19)]
    for kmn in kmns:
        extract_kmn_i4_meta_data(*kmn)
