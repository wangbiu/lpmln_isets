
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-24 21:08
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IConditionsSimplifier.py
"""

from sympy import symbols, simplify, true, false
from sympy.logic.boolalg import And, Or, Not, to_dnf
import lpmln.iset.ISetUtils as isu


def get_dnf_from_iconditions(file, ignore_isets):
    iconditions = isu.load_iconditions_from_file(file)
    iconditions = isu.parse_iconditions_ignore_singletons(iconditions)
    iset_size = len(iconditions[0])
    chars = ["a" + str(i) for i in range(iset_size)]
    symbol_chars = " ".join(chars)
    syms = symbols(symbol_chars)
    print(chars)
    left = false
    right = false
    for ic in iconditions:
        right = get_conjunction_formula(ic, syms, ignore_isets)
        left = Or(left, right)
    return left


def get_conjunction_formula(isp_cdt, syms, ignore_isets):
    left = true
    for i in range(len(syms)):
        if i not in ignore_isets:
            if isp_cdt[i] == 0:
                right = Not(syms[i])
            else:
                right = syms[i]
            left = And(left, right)

    return left


def isp_simplify(file, ignore_isets=set()):
    formula = get_dnf_from_iconditions(file, ignore_isets)
    print(formula)
    simplified = simplify(formula)
    print("simplified: ", simplified)
    # print(to_dnf(left, simplify=True))
    # msg.send_message(str(simplified))
    print("dnf ", to_dnf(simplified))
    return formula


def check_01_distributions_of_iconditions(file):
    iconditions = isu.load_iconditions_from_file(file)
    iconditions = isu.parse_iconditions_ignore_singletons(iconditions)
    iset_size = len(iconditions[0])
    zero_cnts = [0] * iset_size
    one_cnts = [0] * iset_size

    for col in range(iset_size):
        zero_c = 0
        one_c = 0
        for ic in iconditions:
            if ic[col] == 0:
                zero_c += 1
            elif ic[col] == 1:
                one_c += 1
        zero_cnts[col] = zero_c
        one_cnts[col] = one_c

    for col in range(iset_size):
        print("col-%d: zero = %d, one = %d, sum = %d" % (col, zero_cnts[col], one_cnts[col], zero_cnts[col] + one_cnts[col]))

    all_zero_cols = []
    all_ones_cols = []
    one_zero_eq_cols = []
    for col in range(iset_size):
        zero = zero_cnts[col]
        one = one_cnts[col]

        if one == 0:
            print("col-%d all zeros" % col)
            all_zero_cols.append(col)
        elif zero == 0:
            print("col-%d all ones" % col)
            all_ones_cols.append(col)
        elif one == zero:
            one_zero_eq_cols.append(col)

    print("all zeros cols ", all_zero_cols)
    print("all ones cols ", all_ones_cols)
    print("one zero eq cols", one_zero_eq_cols)


def analysis_iconditions_by_nonempty_isets_numbers(icondition_file, min_ne_iset_number, max_ne_iset_number):
    conditions = isu.load_iconditions_from_file(icondition_file)
    conditions = isu.parse_iconditions_ignore_singletons(conditions)
    ne_iset_conditions = dict()
    ne_iset_ids = dict()
    for i in range(min_ne_iset_number, max_ne_iset_number + 1):
        nc = list()
        ne_iset_conditions[i] = nc
        nc = list()
        ne_iset_ids[i] = nc

    for ic in conditions:
        number = isu.get_ne_iset_number(ic)
        if number == 0:
            continue
        ne_iset_conditions[number].append(ic)
        ids = isu.get_ne_iset_ids(ic)
        ne_iset_ids[number].append(ids)

    chains = list()
    for i in range(min_ne_iset_number + 1, max_ne_iset_number + 1):
        for ids in ne_iset_ids[i]:
            chain = find_parent_icondition(ne_iset_ids[i-1], ids)
            if chain == -1:
                print("unchained iconditions non empty isets", ids)
            else:
                chains.append(chain)
                print(chain)

    for i in ne_iset_conditions:
        print("non empty isets number %d, has %d iconditions, iset ids: " % (i, len(ne_iset_conditions[i])), ne_iset_ids[i])


def find_parent_icondition(parent_ne_iset_ids, iset_ids):
    for i in range(len(parent_ne_iset_ids)):
        ids = parent_ne_iset_ids[i]
        if ids.issubset(iset_ids):
            chain = generate_set_chain(ids, iset_ids)
            return chain
    return -1

def generate_set_chain(s1, s2):
    s1 = [str(s) for s in s1]
    s2 = [str(s) for s in s2]
    chain = "{%s} -> {%s}" % (",".join(s1), ",".join(s2))
    return chain




if __name__ == '__main__':
    file_010_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc-non-se.txt"
    file_010_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc.txt"
    file_010_nse2 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-non-se-3sets.txt"
    file_010_asp3_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-asp-non-se-3sets.txt"
    file_010_asp4_nse = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-0-isc-asp-nse.txt"
    file_011_lpmln4_1_6 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-1-6-emp.txt"
    file_011_lpmln4_7_7 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-7-7-emp.txt"
    file_011_asp4_1_6 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-1-6-asp-emp.txt"
    file_011_asp4_7_7 = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-1-1-isc-7-7-asp-emp.txt"


    ignore_isets = {2, 5, 6, 10, 11, 12, 13, 14}
    # isp_simplify(file_010_nse, ignore_isets)

    # check_01_distributions_of_iconditions(file_010_nse)
    # check_01_distributions_of_iconditions(file_010_asp4_nse)
    # isp_simplify(file_010_asp4_nse, ignore_isets)

    # check_01_distributions_of_iconditions(file_010_se)

    file_010_lpmln3_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results-2020.6.8\isc-results\0-1-0-isc-emp.txt"
    file_110_lpmln3_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results-2020.6.8\isc-results\1-1-0-isc-1-24-emp.txt"
    file_011_lpmln3_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results-2020.6.8\isc-results\0-1-1-isc-1-24-emp.txt"
    file_120_lpmln3_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results-2020.6.8\isc-results\1-2-0-isp-1-6-emp.txt"
    file_021_lpmln3_se = r"W:\my_projects\lpmln_isets\isc-data\isc-results\0-2-1-isc-1-33-emp.txt"

    # analysis_iconditions_by_nonempty_isets_numbers(file_110_lpmln3_se, min_ne_iset_number=1, max_ne_iset_number=24)
    # analysis_iconditions_by_nonempty_isets_numbers(file_011_lpmln3_se, min_ne_iset_number=1, max_ne_iset_number=24)
    analysis_iconditions_by_nonempty_isets_numbers(file_021_lpmln3_se, min_ne_iset_number=1, max_ne_iset_number=33)
    check_01_distributions_of_iconditions(file_021_lpmln3_se)
    ignore_isets_021 = { 0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 64, 65, 66, 67, 68, 69, 70, 71, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510}
    print(len(ignore_isets_021))
    isp_simplify(file_021_lpmln3_se, ignore_isets_021)
    # analysis_iconditions_by_nonempty_isets_numbers(file_010_lpmln3_se, min_ne_iset_number=1, max_ne_iset_number=7)
    # analysis_iconditions_by_nonempty_isets_numbers(file_120_lpmln3_se, min_ne_iset_number=1, max_ne_iset_number=6)
    # analysis_iconditions_by_nonempty_isets_numbers(file_011_asp4_7_7, min_ne_iset_number=7, max_ne_iset_number=7)
    pass
    