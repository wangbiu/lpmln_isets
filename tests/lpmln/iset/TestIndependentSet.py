
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/7 10:12
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestIndependentSet.py
"""

from lpmln.iset.IndependentSet import IndependentSet


def test_iset():
    iset = IndependentSet()
    iset.intersect_sets = [1, 2, 3]
    iset.union_sets = [4, 5, 7]
    iset.members = set([1, 3, 9])

    print(iset)


if __name__ == '__main__':
    pass
    