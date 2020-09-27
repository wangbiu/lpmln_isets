
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 10:21
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : RawIConditionUtils.py
"""

import os
import lpmln.config.GlobalConfig as cfg
import pathlib
config = cfg.load_configuration()


def get_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules, postfix):
    rs = 3
    if is_use_extended_rules:
        rs = 4

    file_name = "%s-%d-kmn-%d-%d-%d-%s.txt" % (lp_type, rs, k_size, m_size, n_size, postfix)
    file_path = os.path.join(config.isc_raw_results_path, file_name)
    return file_path


def get_complete_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules):
    postfix = "all"
    return get_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules, postfix)


def get_all_raw_icondition_file_parts(k_size, m_size, n_size, lp_type, is_use_extended_rules, id_begin, id_end):
    files = list()
    for fid in range(id_begin, id_end):
        file_part = get_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules, str(fid))
        files.append(file_part)
    return files


def get_empty_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules, postfix):
    file_path = get_raw_icondition_file(k_size, m_size, n_size, lp_type, is_use_extended_rules, postfix)
    if pathlib.Path(file_path).exists():
        os.remove(file_path)
    return file_path


if __name__ == '__main__':
    pass
    