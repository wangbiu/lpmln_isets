
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



if __name__ == '__main__':
    pass
    