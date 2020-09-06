
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-06 21:53
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISCTasksMetaData.py
"""

class ISCMeta:
    def __init__(self, k_size, m_size, n_size):
        self.kmn = [k_size, m_size, n_size]
        self.is_use_extended_rules = False
        self.unknown_iset_ids = list()
        self.non_se_iset_ids = list()
        self.se_iset_ids = list()

rule2_unknown_isets = [0, 1, 3, 4, 7, 8, 9, 11, 12, 15, 16, 17, 19, 20, 31, 32, 33, 35, 36, 39, 40, 41, 43, 44]
rule3_unknown_isets = [0, 1, 3, 7, 8, 9, 11, 15, 16, 17, 19, 31, 32, 33, 35, 63, 64, 65, 67, 71, 72, 73, 75, 79, 80, 81, 83, 95, 96, 97, 99, 127, 128, 129, 131, 135, 136, 137, 139, 143, 144, 145, 147, 159, 160, 161, 163, 255, 256, 257, 259, 263, 264, 265, 267, 271, 272, 273, 275, 287, 288, 289, 291]

kmn_011_isc = ISCMeta(0, 1, 1)
kmn_011_isc.unknown_iset_ids = rule2_unknown_isets
kmn_011_isc.non_se_iset_ids = [3, 11, 19, 31, 32, 33, 36, 43]
kmn_011_isc.se_iset_ids = [0, 1, 4, 7, 8, 9, 12, 15, 16, 17, 20, 35, 39, 40, 41, 44]

kmn_110_isc = ISCMeta(1, 1, 0)
kmn_110_isc.unknown_iset_ids = rule2_unknown_isets
kmn_110_isc.non_se_iset_ids = [3, 11, 19, 43]
kmn_110_isc.se_iset_ids = [0, 1, 4, 7, 8, 9, 12, 15, 16, 17, 20, 31, 32, 33, 35, 36, 39, 40, 41, 44]

kmn_021_isc = ISCMeta(0, 2, 1)
kmn_021_isc.unknown_iset_ids = rule3_unknown_isets
kmn_021_isc.non_se_iset_ids = [3, 11, 19, 31, 32, 33, 67, 75, 83, 95, 96, 97, 131, 139, 147, 159, 160, 161, 255, 256, 257, 263, 264, 265, 271, 272, 273, 287, 288, 289]
kmn_021_isc.se_iset_ids =  [0, 1, 7, 8, 9, 15, 16, 17, 35, 63, 64, 65, 71, 72, 73, 79, 80, 81, 99, 127, 128, 129, 135, 136, 137, 143, 144, 145, 163, 259, 267, 275, 291]

kmn_111_isc = ISCMeta(1, 1, 1)
kmn_111_isc.unknown_iset_ids = rule3_unknown_isets
kmn_111_isc.non_se_iset_ids = [3, 11, 19, 31, 32, 33, 67, 75, 83, 95, 96, 97, 131, 139, 147, 159, 160, 161]
kmn_111_isc.se_iset_ids = [0, 1, 7, 8, 9, 15, 16, 17, 35, 63, 64, 65, 71, 72, 73, 79, 80, 81, 99, 127, 128, 129, 135, 136, 137, 143, 144, 145, 163, 255, 256, 257, 259, 263, 264, 265, 267, 271, 272, 273, 275, 287, 288, 289, 291]


kmn_120_isc = ISCMeta(1, 2, 0)
kmn_120_isc.unknown_iset_ids = rule3_unknown_isets
kmn_120_isc.non_se_iset_ids = [3, 11, 19, 31, 32, 33, 35, 67, 75, 83, 95, 96, 97, 99, 131, 139, 147, 159, 160, 161, 163]
kmn_120_isc.se_iset_ids = [0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 71, 72, 73, 79, 80, 81, 127, 128, 129, 135, 136, 137, 143, 144, 145, 255, 256, 257, 259, 263, 264, 265, 267, 271, 272, 273, 275, 287, 288, 289, 291]


kmn_210_isc = ISCMeta(2, 1, 0)
kmn_210_isc.unknown_iset_ids = rule3_unknown_isets
kmn_210_isc.non_se_iset_ids = [3, 11, 19, 67, 75, 83, 131, 139, 147]
kmn_210_isc.se_iset_ids = [0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 35, 63, 64, 65, 71, 72, 73, 79, 80, 81, 95, 96, 97, 99, 127, 128, 129, 135, 136, 137, 143, 144, 145, 159, 160, 161, 163, 255, 256, 257, 259, 263, 264, 265, 267, 271, 272, 273, 275, 287, 288, 289, 291]


meta_data = {
    "011": kmn_011_isc,
    "110": kmn_110_isc,
    "021": kmn_021_isc,
    "111": kmn_111_isc,
    "120": kmn_120_isc,
    "210": kmn_210_isc,
}


def report_meta_data():
    for kmn in meta_data:
        data = meta_data[kmn]
        print("%s-itasks has %d se isets " % (kmn, len(data.se_iset_ids)))


def get_kmn_isc_meta_data(k_size, m_size, n_size):
    key = str(k_size) + str(m_size) + str(n_size)
    return meta_data[key]


if __name__ == '__main__':
    report_meta_data()
    data = get_kmn_isc_meta_data(0, 1, 1)
    print(data.se_iset_ids)
    pass
    