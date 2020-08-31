
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-29 16:25
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetCondition.py
"""


class ISetCondition:
    def __init__(self, icondition, singletons):
        self.icondition = icondition
        self.singletom_iset_ids = singletons
        self.iset_number = len(icondition)
        self.ne_iset_ids = set()
        self.contain_se_valid_rules = False
        for i in range(self.iset_number):
            if self.icondition[i] == 1:
                self.ne_iset_ids.add(i)

        self.ne_iset_number = len(self.ne_iset_ids)

    def __str__(self):
        icondition = [str(s) for s in self.icondition]

        if len(self.singletom_iset_ids) == 0:
            return ",".join(icondition)
        else:
            singletons = [str(s) for s in self.singletom_iset_ids]
            condition = "%s:%s" % (",".join(icondition), ",".join(singletons))
            return condition

    def get_ne_iset_str(self):
        isets = [str(s) for s in self.ne_iset_ids]
        return "{%s}" % ",".join(isets)

if __name__ == '__main__':
    pass
    