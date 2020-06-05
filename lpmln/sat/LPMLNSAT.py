
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/4 20:33
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : LPMLNSat.py
"""

from lpmln.sat.ASPSAT import ASPSAT


class LPMLNSAT(ASPSAT):
    @staticmethod
    def se_satisfy_rule(cls, here_interpretation, there_interpretation, rule):
        there_sat_rule = cls.satisfy_rule(cls, there_interpretation, rule)
        if not there_sat_rule:
            return True
        rule_gl_reduct = cls.rule_gl_reduct(cls, there_interpretation, rule)
        if rule_gl_reduct is None:
            return True

        here_sat_rule = cls.satisfy_rule(cls, here_interpretation, rule_gl_reduct)
        return here_sat_rule

    @staticmethod
    def lpmln_reduct(cls, interpretation, program):
        prgs = []
        for rule in program:
            if cls.satisfy_rule(interpretation, rule):
                prgs.append(rule)
        return prgs

    @staticmethod
    def lpmln_gl_reduct(cls, interpretation, program):
        prgs = cls.lpmln_reduct(cls, interpretation, program)
        prgs = cls.gl_reduct(cls, interpretation, prgs)
        return prgs

    @staticmethod
    def se_reduct(cls, interpretation, program):
        return cls.lpmln_gl_reduct(cls, interpretation, program)

if __name__ == '__main__':
    pass
    