
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-06 23:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestSSHClient.py
"""

import os
import lpmln.utils.SSHClient as ssh
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()

server_path = "/home/wangbin/experiments/tmp/"
local_path = "W:/my_projects/lpmln_isets/isc-data/isc-tasks/"


def get_local_file_names(local_path):
    files = os.listdir(local_path)
    file_names = list()
    for f in files:
        file_names.append(str(f))
        # print(f)
    return file_names


def get_threeparts_paths():
    files = get_local_file_names(local_path)
    paths = list()
    for f in files:
        paths.append((local_path, server_path, f))
    for p in paths:
        print(p)
    return paths


def test_transport_files():
    paths = get_threeparts_paths()
    ip = "10.201.186.98"
    ssh.transport_files_by_threeparts_path(ip, 22, config.ssh_user_name, config.ssh_password, paths)


if __name__ == '__main__':
    get_local_file_names(local_path)
    pass
    