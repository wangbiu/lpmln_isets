
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
import pathlib


def get_lock_file(pid):
    return os.path.join(config.isc_non_se_icondition_path, "update-nse-%d.lock" % pid)


def check_lock(pid):
    lock = get_lock_file(pid)
    if os.path.exists(lock):
        return True
    else:
        return False


def get_transport_complete_flag_file(k_size, m_size, n_size, non_empty_iset_number):
    name = "kmn-%d-%d-%d-nse-%d.complete" % (k_size, m_size, n_size, non_empty_iset_number)
    path = os.path.join(config.isc_non_se_icondition_path, name)
    return path


def create_transport_complete_flag_file(k_size, m_size, n_size, non_empty_iset_number):
    path = get_transport_complete_flag_file(k_size, m_size, n_size, non_empty_iset_number)
    pathlib.Path(path).touch()
    return path


def create_and_send_transport_complete_flag_file(k_size, m_size, n_size, non_empty_iset_number, host_ips):
    path = create_transport_complete_flag_file(k_size, m_size, n_size, non_empty_iset_number)
    for ip in host_ips:
        transport_non_se_results([path], ip)


def clear_transport_complete_flag_files(k_size, m_size, n_size, min_ne, max_ne):
    for i in range(min_ne, max_ne+1):
        path = get_transport_complete_flag_file(k_size, m_size, n_size, i)
        if os.path.exists(path):
            os.remove(path)


def join_list_data(data):
    data = [str(d) for d in data]
    return ",".join(data)


def get_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    file_name = get_file_name(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules)
    file_path = os.path.join(config.isc_non_se_icondition_path, file_name)
    return file_path


def get_file_name_prefix(k_size, m_size, n_size, lp_type, is_use_extended_rules):
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    file_name_prefix = "%s-%d-kmn-%d%d%d" % (lp_type, rule_set_size, k_size, m_size, n_size)
    return file_name_prefix


def get_file_name(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    prefix = get_file_name_prefix(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    file_name = "%s-%d-nse.txt" % (prefix, non_empty_iset_number)
    return file_name


def get_all_non_se_file_names(k_size, m_size, n_size, lp_type, is_use_extended_rules):
    prefix = get_file_name_prefix(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    all_files = os.listdir(config.isc_non_se_icondition_path)
    non_se_files = list()
    for f in all_files:
        if f.startswith(prefix):
            non_se_files.append(f)
    return non_se_files


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
    