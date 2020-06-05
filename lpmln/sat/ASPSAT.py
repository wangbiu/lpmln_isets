
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/4 20:22
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ASPSat.py
"""

from lpmln.sat.BaseSAT import BaseSAT


class ASPSAT(BaseSAT):
    @staticmethod
    def se_reduct(cls, interpretation, program):
        return cls.gl_reduct(cls, interpretation, program)

    @staticmethod
    def rule_gl_reduct(cls, interpretation, rule):
        gl_rule = None
        if cls.satisfy_negative_body(interpretation, rule[2]):
            gl_rule = [rule[0], rule[1], []]
        return gl_rule

    @staticmethod
    def gl_reduct(cls, interpretation, program):
        prgs = []
        for rule in program:
            gl_rule = ASPSAT.rule_gl_reduct(cls, interpretation, rule)
            if gl_rule is not None:
                prgs.append(gl_rule)
        return prgs

    @staticmethod
    def se_satisfy_rule(cls, here_interpretation, there_interpretation, rule):
        there_sat_rule = cls.satisfy_rule(cls, there_interpretation, rule)
        if not there_sat_rule:
            return False
        rule_gl_reduct = cls.rule_gl_reduct(cls, there_interpretation, rule)
        if rule_gl_reduct is None:
            return True

        here_sat_rule = cls.satisfy_rule(cls, here_interpretation, rule_gl_reduct)
        return here_sat_rule





if __name__ == '__main__':
    pass
    