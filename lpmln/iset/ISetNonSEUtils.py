
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-07 0:38
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetNonSEUtils.py
"""

import os
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()
import lpmln.utils.SSHClient as ssh


def join_list_data(data):
    data = [str(d) for d in data]
    return ",".join(data)


def get_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    file_name = "%s-%d-kmn-%d%d%d-%d-nse.txt" % (lp_type, rule_set_size, k_size, m_size, n_size, non_empty_iset_number)
    file_path = os.path.join(config.isc_non_se_icondition_path, file_name)
    return file_path


def save_kmn_non_se_results(k_size, m_size, n_size, non_empty_iset_number, conditions, lp_type, is_use_extended_rules):
    file_path = get_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules)
    with open(file_path, mode="w", encoding="utf-8") as f:
        for ic in conditions:
            f.write(join_list_data(ic))
            f.write("\n")
    return file_path


def load_kmn_non_se_results(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    file_path = get_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules)
    conditions = list()
    with open(file_path, mode="w", encoding="utf-8") as f:
        for ic in f:
            data = ic.strip("\r\n").split(",")
            data = [int(d) for d in data]
            conditions.append(set(data))
    return conditions


def transport_non_se_results(files, hosts):
    file_pairs = list()
    for f in files:
        file_pairs.append((f, f))

    for h in hosts:
        send_success = False
        while not send_success:
            try:
                ssh.transport_files(h, 22, config.ssh_user_name, config.ssh_password, file_pairs)
                send_success = True
            except Exception as e:
                print(e)
                send_success = False


if __name__ == '__main__':
    pass
    