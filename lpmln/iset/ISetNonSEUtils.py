
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


def get_task_space_layer_finish_flag_file(k_size, m_size, n_size, layer_id):
    name = "kmn-tq-%d-%d-%d-l%d.finish" % (k_size, m_size, n_size, layer_id)
    return os.path.join(config.isc_non_se_icondition_path, name)


def create_task_space_layer_finish_flag_file(k_size, m_size, n_size, layer_id):
    file_path = get_task_space_layer_finish_flag_file(k_size, m_size, n_size, layer_id)
    pathlib.Path(file_path).touch()
    return file_path


def clear_task_space_layer_finish_flag_files(k_size, m_size, n_size, min_layer_id, max_layer_id):
    for i in range(min_layer_id, max_layer_id + 1):
        file_path = get_task_space_layer_finish_flag_file(k_size, m_size, n_size, i)
        if pathlib.Path(file_path).exists():
            os.remove(file_path)


def get_task_early_terminate_flag_file(k_size, m_size, n_size):
    name = "kmn-%d-%d-%d.terminate" % (k_size, m_size, n_size)
    path = os.path.join(config.isc_non_se_icondition_path, name)
    return path


def create_task_early_terminate_flag_file(k_size, m_size, n_size, working_ne_iset_number):
    path = get_task_early_terminate_flag_file(k_size, m_size, n_size)
    pathlib.Path(path).touch()
    pathlib.Path(path).write_text(str(working_ne_iset_number), encoding="utf-8")
    return path


def create_and_send_task_early_terminate_flag_file(k_size, m_size, n_size, working_ne_iset_number, host_ips):
    path = create_task_early_terminate_flag_file(k_size, m_size, n_size, working_ne_iset_number)
    transport_non_se_results([path], host_ips)


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
    transport_non_se_results([path], host_ips)


def clear_transport_complete_flag_files(k_size, m_size, n_size, min_ne, max_ne):
    for i in range(min_ne, max_ne+1):
        path = get_transport_complete_flag_file(k_size, m_size, n_size, i)
        if os.path.exists(path):
            os.remove(path)


def clear_task_terminate_flag_files(k_size, m_size, n_size):
    flag_file = get_task_early_terminate_flag_file(k_size, m_size, n_size)
    if pathlib.Path(flag_file).exists():
        os.remove(flag_file)


def join_list_data(data):
    data = [str(d) for d in data]
    return ",".join(data)


def get_nse_condition_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    file_name = get_nse_condition_file_name(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules)
    file_path = os.path.join(config.isc_non_se_icondition_path, file_name)
    return file_path


def get_nse_condition_file_name_prefix(k_size, m_size, n_size, lp_type, is_use_extended_rules):
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    file_name_prefix = "%s-%d-kmn-%d-%d-%d" % (lp_type, rule_set_size, k_size, m_size, n_size)
    return file_name_prefix


def get_nse_condition_file_name(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    prefix = get_nse_condition_file_name_prefix(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    file_name = "%s-%d-nse.txt" % (prefix, non_empty_iset_number)
    return file_name


def get_all_non_se_file_names(k_size, m_size, n_size, lp_type, is_use_extended_rules):
    prefix = get_nse_condition_file_name_prefix(k_size, m_size, n_size, lp_type, is_use_extended_rules)
    all_files = os.listdir(config.isc_non_se_icondition_path)
    non_se_files = list()
    for f in all_files:
        if f.startswith(prefix):
            non_se_files.append(f)
    return non_se_files


def save_kmn_non_se_results(k_size, m_size, n_size, non_empty_iset_number, conditions, lp_type, is_use_extended_rules):
    file_path = get_nse_condition_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules)
    with open(file_path, mode="w", encoding="utf-8") as f:
        for ic in conditions:
            f.write(join_list_data(ic))
            f.write("\n")
    return file_path


def load_kmn_non_se_results(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules):
    file_path = get_nse_condition_file_path(k_size, m_size, n_size, non_empty_iset_number, lp_type, is_use_extended_rules)
    conditions = list()
    with open(file_path, mode="r", encoding="utf-8") as f:
        for ic in f:
            data = ic.strip("\r\n").split(",")
            data = [int(d) for d in data]
            conditions.append(set(data))
    return conditions


def transport_non_se_results(files, hosts):
    # print(files)
    # print(hosts)
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
    res = load_kmn_non_se_results(0, 1, 1, 2, "lpmln", False)
    pass
    