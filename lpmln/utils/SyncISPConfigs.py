
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-07 14:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SyncISPConfigs.py
"""

import lpmln.utils.SSHClient as ssh
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()


host_ips = ["10.201.186.98", "10.201.9.220", "10.201.237.52", "10.201.31.76"]

sync_files = [
    (r"W:\my_projects\lpmln_isets\isets-tasks.json", "/home/wangbin/experiments/lpmln_isets/lpmln_isets/isets-tasks.json"),
    (r"W:\my_projects\lpmln_isets\isc-data\isc-tasks\ts-011-1-16.txt", "/home/wangbin/experiments/lpmln_isets/isc-data/isc-tasks/ts-011-1-16.txt"),
    (r"W:\my_projects\lpmln_isets\isc-data\isc-tasks\ts-110-1-20.txt", "/home/wangbin/experiments/lpmln_isets/isc-data/isc-tasks/ts-110-1-20.txt"),
    (r"W:\my_projects\lpmln_isets\isc-data\isc-tasks\ts-111-1-10.txt", "/home/wangbin/experiments/lpmln_isets/isc-data/isc-tasks/ts-111-1-10.txt"),
    (r"W:\my_projects\lpmln_isets\isc-data\isc-tasks\ts-021-1-10.txt", "/home/wangbin/experiments/lpmln_isets/isc-data/isc-tasks/ts-021-1-10.txt"),

]


def sync_all_files(host_ips, files):
    username = config.ssh_user_name
    password = config.ssh_password
    port = 22
    for ip in host_ips:
        ssh.transport_files(ip, port, username, password, files)


if __name__ == '__main__':
    sync_all_files(host_ips, sync_files[0:1])
    pass
    