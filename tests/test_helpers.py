import sys
import os
import unittest
from pigpy.reports import Report, Plan
from pigpy.helpers import filter_report

class test_helpers(unittest.TestCase):
    def setUp(self):
        self.report = Report("to_filter", "%(this)s = LOAD 'bears.txt'")

    def test_basic_filter(self):
        basic = Plan(reports=[filter_report(self.report, "$0 == 0")])
        self.assert_("to_filter" in basic.pigfile)
        self.assert_("$0 == 0" in basic.pigfile)
