
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 15:25
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : LPMLNSEChecking.py
"""
from lpmln.sat.BaseSEChecking import BaseSEChecking
from lpmln.sat.LPMLNSAT import LPMLNSAT
import itertools


class LPMLNSEChecking(BaseSEChecking):

    @staticmethod
    def se_check_program(prg1=list(), prg2=list()):
        universe = LPMLNSEChecking.get_universe_from_programs([prg1, prg2])
        size = len(universe)
        for t_size in range(1, size + 1):
            for there in itertools.combinations(universe, t_size):
                reduct1 = LPMLNSAT.se_reduct(LPMLNSAT, there, prg1)
                reduct2 = LPMLNSAT.se_reduct(LPMLNSAT, there, prg2)

                for h_size in range(t_size):
                    for here in itertools.combinations(there, h_size):
                        sat1 = LPMLNSAT.satisfy_program(LPMLNSAT, here, reduct1)
                        sat2 = LPMLNSAT.satisfy_program(LPMLNSAT, here, reduct2)
                        if sat1 == sat2:
                            continue
                        else:
                            return False
        return True

    @staticmethod
    def se_check_kmn_program(prg_k=list(), prg_m=list(), prg_n=list()):
        universe = LPMLNSEChecking.get_universe_from_programs([prg_k, prg_m, prg_n])

        size = len(universe)
        for t_size in range(1, size + 1):
            for there in itertools.combinations(universe, t_size):
                reductk = LPMLNSAT.se_reduct(LPMLNSAT, there, prg_k)
                reductm = LPMLNSAT.se_reduct(LPMLNSAT, there, prg_m)
                reductn = LPMLNSAT.se_reduct(LPMLNSAT, there, prg_n)

                for h_size in range(t_size):
                    for here in itertools.combinations(there, h_size):
                        sat_k = LPMLNSAT.satisfy_program(LPMLNSAT, here, reductk)

                        if sat_k:
                            sat1 = LPMLNSAT.satisfy_program(LPMLNSAT, here, reductm)
                            sat2 = LPMLNSAT.satisfy_program(LPMLNSAT, here, reductn)
                            if sat1 == sat2:
                                continue
                            else:
                                return False
                        else:
                            continue
        return True


if __name__ == '__main__':
    pass
    