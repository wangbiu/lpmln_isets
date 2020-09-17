
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-17 12:44
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : MultipleProecessing.py
"""
from multiprocessing import Pool, Queue
from multiprocessing.managers import BaseManager
import time

task_queue_list = [Queue(), Queue(), Queue()]
result_queue_list = [Queue(), Queue(), Queue()]

class SearchMasterQueueManger(BaseManager):
    pass


class SearchWorkerQueueManger(BaseManager):
    pass


def get_task_queue(i):
    return task_queue_list[i]


def get_result_queue(i):
    return task_queue_list[i]

host = "127.0.0.1"
port = 7845
passwd = "test123"


def master_fun():
    SearchMasterQueueManger.register("get_task_queue", callable=get_task_queue)
    SearchMasterQueueManger.register("get_result_queue", callable=get_result_queue)
    manager = SearchMasterQueueManger(address=(host, port),
                                      authkey=bytes(passwd, encoding="utf-8"))

    manager.start()
    task_queue = manager.get_task_queue(0)
    result_queue = manager.get_result_queue(0)
    for i in range(5):
        task_queue.put(i)

    while not task_queue.empty():
        time.sleep(1)


def worker_fun():
    SearchWorkerQueueManger.register("get_task_queue")
    SearchWorkerQueueManger.register("get_result_queue")
    manager = SearchWorkerQueueManger(address=(host, port),
                                      authkey=bytes(passwd, encoding="utf-8"))


    manager.connect()
    task_queue = manager.get_task_queue(0)
    result_queue = manager.get_result_queue(0)

    while not task_queue.empty():
        print(task_queue.get())





if __name__ == '__main__':
    # master_fun()
    worker_fun()
    pass
    