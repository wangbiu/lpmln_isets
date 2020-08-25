
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 15:25
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ASPSEChecking.py
"""

from lpmln.sat.BaseSEChecking import BaseSEChecking
from lpmln.sat.ASPSAT import ASPSAT
import itertools


class ASPSEChecking(BaseSEChecking):

    @staticmethod
    def is_se_valid_rule(rule):
        return False

    @staticmethod
    def se_check_program(prg1=list(), prg2=list()):
        universe = ASPSEChecking.get_universe_from_programs(ASPSEChecking, [prg1, prg2])
        size = len(universe)
        for t_size in range(1, size + 1):
            for there in itertools.combinations(universe, t_size):
                reduct1 = ASPSAT.se_reduct(ASPSAT, there, prg1)
                reduct2 = ASPSAT.se_reduct(ASPSAT, there, prg2)

                for h_size in range(t_size):
                    for here in itertools.combinations(there, h_size):
                        sat1 = ASPSAT.satisfy_program(ASPSAT, here, reduct1)
                        sat2 = ASPSAT.satisfy_program(ASPSAT, here, reduct2)
                        if sat1 == sat2:
                            continue
                        else:
                            return False

    @staticmethod
    def se_check_kmn_program(prg_k=list(), prg_m=list(), prg_n=list()):
        universe = ASPSEChecking.get_universe_from_programs(ASPSEChecking, [prg_k, prg_m, prg_n])

        size = len(universe)
        for t_size in range(0, size + 1):
            tit = itertools.combinations(universe, t_size)
            for there in tit:
                reductk = ASPSAT.se_reduct(ASPSAT, there, prg_k)
                reductm = ASPSAT.se_reduct(ASPSAT, there, prg_m)
                reductn = ASPSAT.se_reduct(ASPSAT, there, prg_n)

                t_sat_k = ASPSAT.satisfy_program(ASPSAT, there, reductk)
                if t_sat_k:
                    t_sat_m = ASPSAT.satisfy_program(ASPSAT, there, reductm)
                    t_sat_n = ASPSAT.satisfy_program(ASPSAT, there, reductn)
                    if t_sat_m != t_sat_n:
                        return False
                else:
                    continue

                for h_size in range(t_size):
                    hit = itertools.combinations(there, h_size)
                    for here in hit:
                        homo_sat = ASPSEChecking.interpretation_homo_satisfy_asp_kmn_programs(here, reductk, reductm, reductn)
                        if homo_sat:
                            continue
                        else:
                            return False

        return True

    @staticmethod
    def interpretation_homo_satisfy_asp_kmn_programs(interpretation, prg_k, prg_m, prg_n):
        sat_k = ASPSAT.satisfy_program(ASPSAT, interpretation, prg_k)
        if sat_k:
            sat_m = ASPSAT.satisfy_program(ASPSAT, interpretation, prg_m)
            sat_n = ASPSAT.satisfy_program(ASPSAT, interpretation, prg_n)
            if sat_m == sat_n:
                return True
            else:
                return False
        else:
            return True


if __name__ == '__main__':
    pass
    