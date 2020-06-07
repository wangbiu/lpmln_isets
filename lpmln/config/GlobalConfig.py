#script (python)
# encoding:utf-8
"""
@author: 王彬
@date: 
@describe: 
"""
import configparser
import os
import logging
import logging.config


class GlobalConfiguration:

    def __init__(self):
        self.global_config_file_name = "lpmln-isets.ini"
        self.project_name = "lpmln_isets"
        self.project_base_dir = ""

        self.io_data_dir = ""
        self.isc_tmp_path = "isc-tmp"
        self.isc_task_path = "isc-tasks"
        self.isc_results_path = "isc-results"

        # mattermost
        self.mattermost_username = ""
        self.mattermost_password = ""
        self.mattermost_server = "10.201.23.66"
        self.mattermost_port = 8065
        self.mattermost_api_token = ""
        self.mattermost_channel_id = ""
        self.mattermost_team_id = "e6jfy6m4tr9tpyczycjrjwfye"
        self.mattermost_open = False

        # multiple processing
        self.task_host = ""
        self.task_host_key = ""
        self.worker_hosts = ""
        self.worker_payload = 1
        self.worker_host_name = ""
        self.max_task_works_number = 300
        self.task_host_lock_file = "isc-task.lock"

        # log
        self.log_config_file = "logging.ini"
        self.log_dir = "logs"


    def load_global_configurations(self):
        cf = configparser.ConfigParser()
        cwd_path = os.path.realpath(__file__)

        while not cwd_path.endswith(self.project_name):
            cwd_path = os.path.split(cwd_path)[0]

        self.project_base_dir = cwd_path

        global_config_path = os.path.join(cwd_path, self.global_config_file_name)
        cf.read(global_config_path)

        sections = cf.sections()

        for s in sections:
            for o in cf.options(s):
                self.__setattr__(o, cf.get(s, o))

        if self.mattermost_open == "True":
            self.mattermost_open = True
        else:
            self.mattermost_open = False

        self.task_host_port = int(self.task_host_port)
        self.worker_payload = int(self.worker_payload)
        self.max_task_works_number = int(self.max_task_works_number)


        # log
        self.log_config_file = os.path.join(self.project_base_dir, self.log_config_file)
        self.log_dir = os.path.join(self.project_base_dir, self.log_dir)
        self.create_dirs(self.log_dir)

        self.create_dirs(self.io_data_dir)
        self.isc_tmp_path = os.path.join(self.io_data_dir, self.isc_tmp_path)
        self.create_dirs(self.isc_tmp_path)

        self.isc_task_path = os.path.join(self.io_data_dir, self.isc_task_path)
        self.create_dirs(self.isc_task_path)

        self.isc_results_path = os.path.join(self.io_data_dir, self.isc_results_path)
        self.create_dirs(self.isc_results_path)

    def create_dirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_task_slice_file_path(self, rule_number, min_non_empty_iset_number, max_non_empty_iset_number):
        slice_file = "ts-%d-%d-%d.txt" % (rule_number, min_non_empty_iset_number, max_non_empty_iset_number)
        slice_file = os.path.join(self.isc_task_path, slice_file)
        return slice_file

    def get_isc_results_file_path(self, k_size, m_size, n_size, min_non_empty_iset_number, max_non_empty_iset_number):
        result_file = "%d-%d-%d-isc-%d-%d-emp.txt" % (
            k_size, m_size, n_size, min_non_empty_iset_number, max_non_empty_iset_number)
        result_file = os.path.join(self.isc_results_path, result_file)
        return result_file


def load_configuration():
    config = GlobalConfiguration()
    config.load_global_configurations()
    old_cwd = os.getcwd()
    os.chdir(config.project_base_dir)
    logging.config.fileConfig(config.log_config_file)
    os.chdir(old_cwd)
    return config


if __name__ == '__main__':
    config = load_configuration()
    logging.info("test config")
    print(config)


