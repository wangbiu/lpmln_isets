
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/7 10:12
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestIndependentSet.py
"""

from lpmln.iset.IndependentSet import IndependentSet


def test_iset_1():
    iset = IndependentSet()
    iset.intersect_sets = [0, 2, 3]
    iset.union_sets = [4, 5, 1]
    iset.members = set([1, 3, 9])

    print(iset)
    print(iset.stringfy_iset_condition_rule())
    print(iset.stringfy_iset_condition_tex())


def test_iset_2():
    iset = IndependentSet()
    iset.intersect_sets = [2, 3, 4]
    iset.union_sets = [0, 1, 5]
    iset.members = set(['a'])

    print(iset)
    print(iset.stringfy_iset_condition_rule())
    print(iset.stringfy_iset_condition_tex())


if __name__ == '__main__':
    pass
    