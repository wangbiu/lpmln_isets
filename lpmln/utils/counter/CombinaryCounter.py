#script (python)
# encoding:utf-8
"""
@author: 王彬
@date: 
@describe: 
"""

from lpmln.utils.counter.BaseCounter import BaseCounter
import copy

class CombinaryCounter(BaseCounter):
    def __init__(self):
        super.__init__()
        self.max_capacity = 0

    def __init__(self, digits, max_capacity):
        self.digits = digits
        self.max_capacity = max_capacity
        self.init_max_boundary()
        self.init_counter()

    def init_max_boundary(self):
        self.max_boundary = [self.max_capacity - self.digits + i for i in range(self.digits)]

    def reset_real_max_boundary(self, boundary):
        self.real_boundary = copy.deepcopy(boundary)


    def init_counter(self):
        self.is_first_run = True
        self.real_boundary = copy.deepcopy(self.max_boundary)
        self.current_indicator = [i for i in range(self.digits)]

    def set_currentindicator_after_addone(self, index):
        init_val = self.current_indicator[index - 1]
        for i in range(index, self.digits):
            init_val += 1
            self.current_indicator[i] = init_val

    @staticmethod
    def compute_comb(n, m):
        part1 = CombinaryCounter.product(n - m + 1, n)
        part2 = CombinaryCounter.product(1, m)
        return part1 // part2

    @staticmethod
    def product(i, j):
        result = 1
        for k in range(i, j + 1):
            result *= k
        return result

if __name__ == '__main__':
    counter = CombinaryCounter(2, 7)
    indicator = []

    while indicator is not None:
        print(indicator)
        indicator = counter.get_current_indicator()

