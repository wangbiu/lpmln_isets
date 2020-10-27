
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-10-06 16:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : KMN021HT.py
"""

from lpmln.iset.ParallelISetConditionValidator import ParallelIConditionValidator

k_size = 2
m_size = 1
n_size = 0
lp_type = "lpmln"
is_use_extended_rules = False


def check_ne_27_1():
    ne_isets = {256,257,0,1,129,64,128,65,264,8,136,72,273,17,145,81,32,160,288,35,163,291,96,99,127,63,255}
    # ne_isets = {32,257,160,35,0,1,129,65}
    validator = ParallelIConditionValidator(is_use_extended_rules, lp_type)
    validator.parallel_validate(ne_isets, k_size, m_size, n_size, False)


def check_ne_27_2():
    ne_isets = {256,0,1,259,129,64,7,264,8,9,267,137,72,15,272,16,17,275,145,80,31,32,33,161,288,291,96}
    validator = ParallelIConditionValidator(is_use_extended_rules, lp_type)
    validator.parallel_validate(ne_isets, k_size, m_size, n_size, False)



if __name__ == '__main__':
    check_ne_27_1()
    pass
    