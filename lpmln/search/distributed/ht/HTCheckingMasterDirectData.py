
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-29 16:23
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : HTCheckingMasterDirectData.py
"""

from lpmln.search.distributed.ht.HTCheckingMaster import HTCheckingMaster
from lpmln.search.distributed.ht.HTCheckingWorkerDirectData import HTCheckingDirectWorker
import logging
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.RawIConditionUtils as riu
import lpmln.iset.ISetNonSEUtils as isnse
import lpmln.utils.SSHClient as ssh
import copy
import pathlib
from lpmln.search.distributed.final.FinalSearchBase import SearchQueueManager, ITaskSignal
config = cfg.load_configuration()


class HTCheckingDirectMaster(HTCheckingMaster):
    @staticmethod
    def itask_slices_generator(cls, isc_config_file):
        msg_text = "%s init task slices generator ..." % str(cls)
        logging.info(msg_text)
        msg.send_message(msg_text)

        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_worker_queue_manager()

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks
        batch_size = 100

        for tid in range(len(isc_tasks)):
            it = isc_tasks[tid]
            data_file = riu.get_complete_raw_icondition_file(*it.k_m_n, it.lp_type, it.is_use_extended_rules)
            data_cnt = 0
            data_batch = list()
            with open(data_file, mode="r", encoding="utf-8") as df:
                for data in df:
                    data_cnt += 1
                    data_batch.append(data)
                    if data_cnt == batch_size:
                        data_cnt = 0
                        send_tuple = (tid, tuple(data_batch))
                        task_queue.put(send_tuple)
                        data_batch = list()

                if len(data_batch) > 0:
                    send_tuple = (tid, tuple(data_batch))
                    task_queue.put(send_tuple)

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            ht_task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")

    @staticmethod
    def init_pre_task_worker_pool(cls, isc_config_file, result_queue):
        worker_pool, result_queue, host_ip = HTCheckingDirectWorker.inner_init_kmn_isc_task_workers(
            HTCheckingDirectWorker, isc_config_file, is_check_valid_rules=False, result_queue=result_queue)
        return worker_pool

if __name__ == '__main__':
    pass
    