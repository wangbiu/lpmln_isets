
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 16:23
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ITaskMetaChecker.py
"""

from lpmln.itask.ITaskMeta import ITaskMeta, ITaskMetaGenerator
import lpmln.config.GlobalConfig as cfg
import lpmln.iset.ISetNonSEUtils as isnse
config = cfg.load_configuration()


def load_itask_meta_data(k_size, m_size, n_size, lp_type="lpmln", is_use_extended_rules=False):
    rs = 3
    if is_use_extended_rules:
        rs = 4

    meta_key = "%d-%d-%d-%s-%d" % (k_size, m_size, n_size, lp_type, rs)
    file = config.isc_meta_data_file
    all_meta_data = ITaskMeta.load_itask_meta_data_from_file(file)
    data = all_meta_data[meta_key]
    return data


def load_all_nse_condition_files(k_size, m_size, n_size, max_ne, lp_type="lpmln", is_use_extended_rules=False):
    nse_conditions = list()
    for i in range(1, max_ne + 1):
        cdts = isnse.load_kmn_non_se_results(k_size, m_size, n_size, i, lp_type, is_use_extended_rules)
        nse_conditions.extend(cdts)

    print("load %d nse conditions for %d-%d-%d %s-%s itask" % (len(nse_conditions), k_size, m_size, n_size, lp_type, str(is_use_extended_rules)))
    return nse_conditions


def check_itask(k_size, m_size, n_size, lp_type="lpmln", is_use_extended_rules=False):
    data = load_itask_meta_data(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    print(data)

    max_ne = len(data.search_space_iset_ids)
    nse_conditions = load_all_nse_condition_files(k_size, m_size, n_size, max_ne, lp_type, is_use_extended_rules)

    nse_condition_isets = set()
    for nse in nse_conditions:
        nse_condition_isets = nse_condition_isets.union(set(nse))

    search_i4_isets = set(data.search_i4_composed_iset_ids)
    non_i4_nse_condition_isets = nse_condition_isets.difference(search_i4_isets)
    print("has %d search i4 isets: " % len(search_i4_isets), search_i4_isets)
    print("nse condition has %d isets: " % len(nse_condition_isets), nse_condition_isets)
    print("nse condition has %d non-i4 isets: " % len(non_i4_nse_condition_isets), non_i4_nse_condition_isets)
    print("minimal i4 tuples: ", data.minmal_i4_isets_tuples)

    freq = compute_iset_in_icondition_frequency(non_i4_nse_condition_isets, nse_conditions)
    print("nse condition non-i4 iset frequency: ")
    for k in freq:
        print(" %d : %d " %  (k, freq[k]))


def compute_iset_in_icondition_frequency(isets, conditions):
    freq = dict()
    for i in isets:
        freq[i] = 0

    for ic in conditions:
        for iset in ic:
            if iset in freq:
                freq[iset] += 1

    return freq


def check_itask_meta(k_size, m_size, n_size, lp_type="lpmln", is_use_extended_rules=False):
    data = load_itask_meta_data(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    print(data)

    search_i4_isets = set(data.search_i4_composed_iset_ids)
    print("has %d search i4 isets: " % len(search_i4_isets), search_i4_isets)
    print("minimal i4 tuples: ", data.minmal_i4_isets_tuples)


if __name__ == '__main__':
    # check_itask(0, 2, 1)
    check_itask_meta(0, 1, 1)
    pass
    