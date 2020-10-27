
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-29 16:25
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetCondition.py
"""


class ISetCondition:
    def __init__(self, icondition, singletons, is_save_mem=True):
        self.icondition = icondition
        self.singletom_iset_ids = singletons
        self.iset_number = len(icondition)
        self.ne_iset_ids = set()
        self.contain_se_valid_rules = False
        self.is_save_mem = is_save_mem
        for i in range(self.iset_number):
            if self.icondition[i] == 1:
                self.ne_iset_ids.add(i)

        self.ne_iset_number = len(self.ne_iset_ids)
        if self.is_save_mem:
            self.icondition = None

    def __str__(self):
        ne_isets = [str(s + 1) for s in self.ne_iset_ids]
        if len(self.singletom_iset_ids) == 0:
            return ",".join(ne_isets)
        else:
            singletons = [str(s + 1) for s in self.singletom_iset_ids]
            string = "%s:%s" % (",".join(ne_isets), ",".join(singletons))
            return string

    def set_ne_iset_ids(self, ne_isets):
        self.ne_iset_ids = ne_isets
        self.ne_iset_number = len(ne_isets)

    def stringify(self):
        """
        do not use this function
        """
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
    