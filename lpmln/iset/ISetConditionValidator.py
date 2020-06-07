
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/7 13:04
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetConditionValidator.py
"""

import lpmln.iset.ISetUtils as isu
from lpmln.sat.LPMLNSEChecking import LPMLNSEChecking
from lpmln.sat.ASPSEChecking import ASPSEChecking


class ISetConditionValidator():
    def __int__(self, lp_type="lpmln"):
        if lp_type == "asp":
            self.lp_se = ASPSEChecking
            raise RuntimeError("not support for now")
        elif lp_type == "lpmln":
            self.lp_se = LPMLNSEChecking
        else:
            raise RuntimeError("unknown SE checking logic: %s" % lp_type)

    def validate_isets_kmn_program(self, isets, k_size, m_size, n_size):
        kmn = isu.construct_kmn_program_from_isets(isets, k_size, m_size, n_size)
        is_contain_valid_rule = False
        is_se_sat = False
        for prg in kmn:
            if self.lp_se.is_contain_se_valid_rule(prg):
                is_contain_valid_rule = True
                break

        if is_contain_valid_rule:
            return is_contain_valid_rule, is_se_sat

        is_se_sat = self.lp_se.se_check_kmn_program(*kmn)
        return is_contain_valid_rule, is_se_sat



if __name__ == '__main__':
    pass
    