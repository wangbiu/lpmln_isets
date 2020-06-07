
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/7 18:07
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SearchingInit.py
"""

import lpmln.search.misc.ISCSearchingSlicesGenerator as isg


def init():
    isg.generate_isp_slices(rule_number=2, min_non_empty_iset_number=1, max_non_empty_iset_number=6)
    isg.generate_isp_slices(rule_number=2, min_non_empty_iset_number=7, max_non_empty_iset_number=7)


if __name__ == '__main__':
    init()
    pass
    