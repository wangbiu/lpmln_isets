
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-10 5:24
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : CompareSEChecking.py
"""

import lpmln.sat.LPMLNSEChecking as ht
import lpmln.experiments.TupleGenerator as tg
import datetime

kmns = [(0, 1, 0), (0, 1, 1), (1, 1, 0)]


def compare_kmn_se_checking(atom_size, tuple_size=1000000):
    ht_total_time = 0
    syn_total_time = 0

    first_run = True
    report = []
    for kmn in kmns:
        tuples = tg.get_kmn_tuples(*kmn, atom_size=atom_size, tuple_size=tuple_size)

        ht_se_res = list()
        syn_se_res = list()

        # print("start ht checking")
        ht_start = datetime.datetime.now()
        for tp in tuples:
            # print(tp)
            is_ht_se = ht.LPMLNSEChecking.se_check_kmn_program(*tp)
            ht_se_res.append(is_ht_se)
        ht_end = datetime.datetime.now()
        # print("end ht checking")
        #
        # print("start syntactic checking")
        syn_start = datetime.datetime.now()
        for tp in tuples:
            is_syn_se = tg.check_se_by_se_condition(*tp)
            syn_se_res.append(is_syn_se)
        syn_end = datetime.datetime.now()

        if first_run:
            ht_total_time = ht_end - ht_start
            syn_total_time = syn_end - syn_start
            first_run = False
        else:
            ht_total_time = (ht_end - ht_start) + ht_total_time
            syn_total_time = (syn_end - syn_start) + syn_total_time

        ht_time = str(ht_end - ht_start)
        syn_time = str(syn_end - syn_start)

        msg = "\t\t%d-%d-%d: running time ht %s, syn %s" % (kmn[0], kmn[1], kmn[2], ht_time, syn_time)
        report.append(msg)

    summary = "atom size %d, tuple size %d, running time ht %s, syn %s \n %s" % (
        atom_size, tuple_size, str(ht_total_time), str(syn_total_time), "\n".join(report))

    print(summary)


def experiments(min_atom_size, max_atom_size, tuple_size=1000000):
    for i in range(min_atom_size, max_atom_size+1):
        compare_kmn_se_checking(i, tuple_size)


if __name__ == '__main__':
    experiments(1, 10, tuple_size=1000000)
    pass
    