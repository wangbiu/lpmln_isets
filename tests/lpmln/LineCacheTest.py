
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 23:24
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : LineCacheTest.py
"""

import lpmln.iset.RawIConditionUtils as riu
import linecache

def test_line_cache():
    data_file = riu.get_complete_raw_icondition_file(0, 1, 1, "lpmln", False)
    for i in range(0, 1200):
        print(i, linecache.getline(data_file, i))


if __name__ == '__main__':
    pass
    