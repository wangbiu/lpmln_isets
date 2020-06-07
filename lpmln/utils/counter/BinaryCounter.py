#script (python)
# encoding:utf-8
"""
@author: 王彬
@date: 
@describe: 
"""

from lpmln.utils.counter.BaseCounter import BaseCounter

class BinaryCounter(BaseCounter):

    def __init__(self):
        super.__init__()

    def __init__(self, digits):
        self.digits = digits
        self.max_boundary = [1] * self.digits
        self.init_counter()


if __name__ == '__main__':
    counter = BinaryCounter(3)
    indicator = []

    while indicator is not None:
        print(indicator)
        indicator = counter.get_current_indicator()
        
