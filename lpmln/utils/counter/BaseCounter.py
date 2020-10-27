#script (python)
# encoding:utf-8
"""
@author: 王彬
@date: 
@describe: 
"""

import copy

class BaseCounter:

    def __init__(self):
        self.digits = 0
        self.max_boundary = []
        self.current_indicator = []
        self.is_first_run = True
        self.real_boundary = []
        self.init_counter()

    def __init__(self, digits, max_boundary):
        self.digits = digits
        self.max_boundary = max_boundary
        self.init_counter()

    def init_counter(self):
        self.current_indicator = [0] * self.digits
        self.real_boundary = copy.deepcopy(self.max_boundary)
        self.is_first_run = True

    def reset_current_indicator(self, indicator):
        self.current_indicator = copy.deepcopy(indicator)
        self.is_first_run = True

    def reset_counter(self):
        self.init_counter()

    def next_indicator(self):
        index = self.digits
        cur_val = 1
        cur_boundary = 0

        while cur_val > cur_boundary and index >= 0:
            index -= 1
            cur_val = self.current_indicator[index] + 1
            cur_boundary = self.real_boundary[index]


        if cur_val <= cur_boundary:
            self.current_indicator[index] = cur_val
            self.set_currentindicator_after_addone(index+1)
        else:
            self.current_indicator = None

    def set_currentindicator_after_addone(self, index):
        for i in range(index, self.digits):
            self.current_indicator[i] = 0

    def get_current_indicator(self):
        if self.is_first_run:
            self.is_first_run = False
        else:
            self.next_indicator()

        return copy.deepcopy(self.current_indicator)

    def get_counter_str(self, counter):
        strs = [str(s) for s in counter]
        return "".join(strs)



if __name__ == '__main__':
    digits = 4
    boundary = [3] * 4
    counter = BaseCounter(4, boundary)
    indicator = []
    cnt = 0
    while indicator is not None:
        indicator = counter.get_current_indicator()
        cnt += 1
        print(indicator)

    print("has %d counter index" % cnt )




