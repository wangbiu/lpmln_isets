
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/5/10 20:38
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : BaseSAT.py
"""

import abc


"""
a rule is a tuple of sets (head, positive_body, negative_body)
type of head, postive_body, negative_body is set in Python  
"""


class BaseSAT(abc.ABC):
    def __int__(self):
        pass

    @staticmethod
    def satisfy_head(interpretation, head):
        itp = set(interpretation)
        h = set(head)
        ist = itp.intersection(h)
        if len(ist) != 0:
            return True
        else:
            return False

    @staticmethod
    def satisfy_positive_body(interpretation, pos_body):
        itp = set(interpretation)
        pb = set(pos_body)
        if pb.issubset(itp):
            return True
        else:
            return False

    @staticmethod
    def satisfy_negative_body(interpretation, neg_body):
        itp = set(interpretation)
        nb = set(neg_body)
        ist = itp.intersection(nb)
        if len(ist) == 0:
            return True
        else:
            return False

    @staticmethod
    def satisfy_rule(cls, interpretation, rule):
        head = rule[0]
        pb = rule[1]
        nb = rule[2]

        sat_head = cls.satisfy_head(interpretation, head)
        not_sat_pb = not cls.satisfy_positive_body(interpretation, pb)
        not_sat_nb = not cls.satisfy_negative_body(interpretation, nb)

        if sat_head or not_sat_pb or not_sat_nb:
            return True
        else:
            return False

    @staticmethod
    def satisfy_program(cls, interpretation, program):
        for rule in program:
            if not cls.satisfy_rule(cls, interpretation, rule):
                return False
        return True

    @staticmethod
    @abc.abstractmethod
    def se_satisfy_rule(cls, here_interpretation, there_interpretation, rule):
        pass

    @staticmethod
    @abc.abstractmethod
    def se_reduct(cls, interpretation, program):
        pass

    @staticmethod
    def se_satisfy_program(cls, here_interpretation, there_interpretation, program):
        for rule in program:
            se_sat = cls.se_satisfy_rule(cls, here_interpretation, there_interpretation, rule)
            if not se_sat:
                return False
        return True









if __name__ == '__main__':
    pass
    