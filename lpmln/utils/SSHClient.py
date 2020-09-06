
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-06 23:04
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SSHClient.py
"""

import paramiko
import os


def sftp_transport_file_by_threeparts_path(sftp, local_path, server_path, file_name):
    local_ab_path = local_path + file_name
    server_ab_path = server_path + file_name
    sftp_transport_file(sftp, local_ab_path, server_ab_path)


def sftp_transport_file(sftp, local_ab_path, server_ab_path):
    try:
        sftp.put(local_ab_path, server_ab_path)
    except Exception as e:
        raise RuntimeError(e)


def get_ssh_connection(ip, port, username, password):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, port, username, password)
        return ssh
    except Exception as e:
        raise RuntimeError(e)


def get_sftp_connection(ip, port, username, password):
    try:
        t = paramiko.Transport(sock=(ip, port))
        t.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(t)
        return sftp, t
    except Exception as e:
        raise RuntimeError(e)


def transport_files(ip, port, username, password, files):
    sftp, transport = get_sftp_connection(ip, port, username, password)
    for fp in files:
        local = fp[0]
        server = fp[1]
        sftp_transport_file(sftp, local, server)
    # sftp.close()
    transport.close()


def transport_files_by_threeparts_path(ip, port, username, password, files):
    paths = list()
    for fp in files:
        local = fp[0] + fp[2]
        server = fp[1] + fp[2]
        paths.append((local, server))

    transport_files(ip, port, username, password, paths)


if __name__ == '__main__':

    pass
    