
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-25 21:35
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IConditionUtils.py
"""


from lpmln.iset.ISetCondition import ISetCondition
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()


def load_iconditions_from_file(ic_file, is_ne_formate=True):
    conditions = list()
    if is_ne_formate:
        parse = parse_ne_formate_icondition
    else:
        parse = parse_01_formate_icondition
    with open(ic_file, encoding="utf-8", mode="r") as icf:
        for ic in icf:
            ic = parse(ic)
            conditions.append(ic)

    return conditions


def parse_ne_formate_icondition(data):
    cdt, singleton = parse_raw_icondition_data(data)
    condition = ISetCondition(list(), list(), True)
    condition.set_ne_iset_ids(set(cdt))
    condition.singletom_iset_ids = set(singleton)


def parse_01_formate_icondition(data):
    cdt, singleton = parse_raw_icondition_data(data)
    condition = ISetCondition(cdt, singleton, False)
    return condition


def parse_raw_icondition_data(data):
    data = data.split(":")
    cdt = data[0].split(",")
    cdt = [int(s) for s in cdt]
    if len(data) == 1:
        singleton = list()
    else:
        singleton = data[1].split(",")
        singleton = [int(s) for s in singleton]

    return cdt, singleton



if __name__ == '__main__':
    pass
    