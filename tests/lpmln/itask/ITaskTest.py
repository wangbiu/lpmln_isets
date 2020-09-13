
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 16:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ITaskTest.py
"""

import lpmln.config.ISCTasksMetaData as im
from lpmln.itask.ITaskMeta import ITaskMetaGenerator


def test_generator():
    kmns = [[0, 1, 1], [1, 1, 0], [0, 2, 1], [1, 1, 1], [1, 2, 0], [2, 1, 0]]
    for kmn in kmns:
        print("checking %d-%d-%d results..." % (kmn[0], kmn[1], kmn[2]))
        generator = ITaskMetaGenerator(kmn, "lpmln", False)
        real_data = im.get_kmn_isc_meta_data(*kmn)
        non_se_isets_g = generator.meta_data.non_se_iset_ids
        non_se_isets_r = set(real_data.non_se_iset_ids)

        search_g = generator.meta_data.search_space_isets
        search_r = set(real_data.se_iset_ids)

        non_se_same = False
        search_same = False

        if len(non_se_isets_g) == len(non_se_isets_r) and len(non_se_isets_g.difference(non_se_isets_r)) == 0:
            non_se_same = True

        if len(search_r) == len(search_g) and len(search_g.difference(search_r)) == 0:
            search_same = True

        print("\t is non se isets same? ", non_se_same)
        print("\t is search isets same? ", search_same)

        if not non_se_same or not search_same:
            raise RuntimeWarning("wrong case ", kmn)

        print("\n")





if __name__ == '__main__':
    pass
    