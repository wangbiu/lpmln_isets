
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 16:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ITaskTest.py
"""

import lpmln.config.ISCTasksMetaData as im
from lpmln.itask.ITaskMeta import ITaskMetaGenerator
import lpmln.config.GlobalConfig as cfg
import json

config = cfg.load_configuration()
kmns = [[0, 1, 1], [1, 1, 0], [0, 2, 1], [1, 1, 1], [1, 2, 0], [2, 1, 0]]

def test_generator():
    for kmn in kmns[1:]:
        print("checking %d-%d-%d results..." % (kmn[0], kmn[1], kmn[2]))
        generator = ITaskMetaGenerator(kmn, "lpmln", False)
        real_data = im.get_kmn_isc_meta_data(*kmn)
        non_se_isets_g = generator.meta_data.non_se_iset_ids
        non_se_isets_r = set(real_data.non_se_iset_ids)

        search_g = generator.meta_data.search_space_iset_ids
        search_r = set(real_data.se_iset_ids)

        min_i4_g = generator.meta_data.minmal_i4_isets_tuples
        kmn_key = "%d-%d-%d" % tuple(kmn)
        min_i4_r = load_i4_data(kmn_key)
        min_i4_same = compare_i4_tuples(min_i4_g, min_i4_r)

        non_se_same = False
        search_same = False

        if len(non_se_isets_g) == len(non_se_isets_r) and len(non_se_isets_g.difference(non_se_isets_r)) == 0:
            non_se_same = True

        if len(search_r) == len(search_g) and len(search_g.difference(search_r)) == 0:
            search_same = True

        print("\t is non se isets same? ", non_se_same)
        print("\t is search isets same? ", search_same)
        print("\t is min i4 isets tuple same? ", min_i4_same)

        if not non_se_same or not search_same or not min_i4_same:
            raise RuntimeWarning("wrong case ", kmn)

        print("\n")




def load_i4_data(key):
    file = config.isc_meta_data_file
    with open(file, mode="r", encoding="utf-8") as f:
        data = json.load(f)
        i4_data = data[key]["i4-tuples"]

    return i4_data


def compare_i4_tuples(tp1, tp2):
    tp1 = {stringfy_i4_isets(s) for s in tp1}
    tp2 = {stringfy_i4_isets(s) for s in tp2}

    if len(tp1) == len(tp2) and len(tp1.difference(tp2)) == 0:
        return True
    else:
        return False


def stringfy_i4_isets(i4_isets):
    i4_isets = list(i4_isets)
    i4_isets.sort()
    i4_isets = [str(s) for s in i4_isets]
    return "-".join(i4_isets)


if __name__ == '__main__':
    pass
    