
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 11:03
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : BaseSEChecking.py
"""

import abc


class BaseSEChecking(abc.ABC):

    @staticmethod
    def get_universe(program):
        universe = set()
        for rule in program:
            for part in rule:
                for lit in part:
                    universe.add(lit)
        return universe

    @staticmethod
    def get_universe_from_programs(cls, programs=list()):
        universe = set()
        for prg in programs:
            univ = cls.get_universe(prg)
            universe = universe.union(univ)
        return universe

    @staticmethod
    @abc.abstractmethod
    def se_check_program(prg1=list(), prg2=list()):
        pass

    @staticmethod
    @abc.abstractmethod
    def se_check_kmn_program(prg_k=list(), prg_m=list(), prg_n=list()):
        pass

    @staticmethod
    @abc.abstractmethod
    def is_se_valid_rule(rule):
        pass

    @staticmethod
    def is_contain_se_valid_rule(cls, program):
        for rule in program:
            if cls.is_se_valid_rule(rule):
                return True
        return False




if __name__ == '__main__':
    pass
    