
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 14:09
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestPerformance.py
"""

import itertools
from datetime import datetime


def generator(atom_size):
    universe = [i for i in range(atom_size)]
    sets = list()
    for i in range(0, atom_size + 1):
        it = itertools.combinations(universe, i)
        for si in it:
            sets.append(set(si))

    return sets


def check_relation(atom_size):
    print("generate all sets ...")
    sets = generator(atom_size)
    print("generate all sets ... ok!")

    start = datetime.now()

    for s1 in sets:
        for s2 in sets:
            s1.issubset(s2)

    end = datetime.now()

    print("sets size %d, running time %s" % (len(sets), str(end - start)))




if __name__ == '__main__':
    check_relation(15)
    pass
    