import sys
sys.path.insert(0, "..")
import os
import unittest
import re

from pigpy.reports import Report, Plan

class test_reports(unittest.TestCase):
    def setUp(self):
        pass

    def test_simple_plan_ordering(self):
        one = Report("1", "%(this)s")
        two = Report("2", "%(this)s", parents={"1": one})
        three = Report("3", "%(this)s", parents={"2": two})
        four = Report("4", "%(this)s", parents={"3": three})
        
        plan = Plan(reports=[three], output_directory="/", save_format="")
        self.failUnless(re.match(".*1.*2.*3.*", plan.pigfile, flags=re.DOTALL),
            "Pigfile should match .*1.*2.*3.*, instead is:\n%s" % plan.pigfile
        )
        
        plan.add(four)
        self.failUnless(re.match(".*1.*2.*3.*4.*", plan.pigfile, flags=re.DOTALL),
            "Pigfile should match .*1.*2.*3.*4.*, instead is:\n%s" % plan.pigfile
        )
        

    def test_plan_partial_ordering(self):
        one = Report("1", "%(this)s")
        two = Report("2", "%(this)s", parents={"1": one})
        three = Report("3", "%(this)s", parents={"2": two})
        four = Report("4", "%(this)s", parents={"2": two})
        five = Report("5", "%(this)s", parents={"3": three})
        
        plan = Plan(reports=[three, four])
        self.failUnless(re.match("(.*1.*2)(.*3.*4.*)|(.*4.*3.*)", plan.pigfile,
            flags=re.DOTALL))
        
        plan.add(five)
        self.failUnless(re.match("(.*1.*2)(((.*3.*4.*5.*)|(.*4.*3.*5.*))|(.*3.*5.*4.*))", 
            plan.pigfile, flags=re.DOTALL))
        
    def test_no_cache_columns_also_bypasses_save_of_intermediate_report(self):
        pass
        
    def test_parents_get_filled_into_code_by_name(self):
        base_report = Report("my_name", "")
        child_report = Report("child", 
                              "%(this)s=%(parent_report)s", 
                              parents={"parent_report": base_report})
        plan = Plan(reports=[child_report])
        
        self.failUnless("child=my_name" in plan.pigfile,
            "Parent report %s did not get properly filled into %s report" %
             (base_report, child_report)
        )
        
    def test_reports_in_plan_get_saved(self):
        base_report = Report("not_saved", "some pig code")
        saved_report = Report("saved", "%(this)s = More pig code", parents={"base": base_report})

        plan = Plan(reports=[saved_report])
        
        self.failUnless(re.match(".*STORE.*saved.*INTO.*saved.*", plan.pigfile, flags=re.DOTALL),
            "Report %s should have a save statement. Pigfile is:\n%s" % 
            (saved_report, plan.pigfile)
        )
        
    def test_report_with_dependencies_get_saves(self):
        base_report = Report("cached", "some pig code", cache_columns=["bears", "narwhal"])
        first_child = Report("child1", "", parents={"parent": base_report})
        second_child = Report("child2", "", parents={"parent": base_report})
        
        plan = Plan(reports=[first_child, second_child])

        self.failUnless(re.match(".*STORE.*cached.*LOAD.*cached.*bears.*narwhal.*",
            plan.pigfile, flags=re.DOTALL),
            "Report %s should have been saved and loaded, pigfile is:\n%s" % 
            (base_report, plan.pigfile)
        )

    def test_report_with_multiple_childre_but_no_dependencies_does_not_get_cached(self):
        base_report = Report("cached", "some pig code", cache_columns=["bears", "narwhal"])
        first_child = Report("child1", "", parents={"parent": base_report})
        second_child = Report("child2", "", parents={"parent": base_report})

        plan = Plan(reports=[first_child])

        self.failIf(re.match(".*STORE.*cached.*LOAD.*cached.*bears.*narwhal.*",
            plan.pigfile, flags=re.DOTALL),
            "Report %s should not have been cached, pigfile is:\n%s" % 
            (base_report, plan.pigfile)
        )
        
    def test_report_with_cached_child_does_not_get_cached(self):
        base_report = Report("not_cached", "some pig code", cache_columns=["bears", "narwhal"])
        cached_report = Report("cached", "cached_code", cache_columns=["some_column", "another"])
        first_child = Report("child1", "", parents={"parent": cached_report})
        second_child = Report("child2", "", parents={"parent": cached_report})

        plan = Plan(reports=[first_child, second_child])

        self.failIf(re.match(".*STORE.*not_cached.*LOAD.*not_cached.*bears.*narwhal.*",
            plan.pigfile, flags=re.DOTALL),
            "Report %s should not have been cached, pigfile is:\n%s" % 
            (base_report, plan.pigfile)
        )
        self.failUnless(re.match(".*STORE.*cached.*LOAD.*cached.*some_column.*another.*",
            plan.pigfile, flags=re.DOTALL),
            "Report %s should have been saved and loaded, pigfile is:\n%s" % 
            (base_report, plan.pigfile)
        )

    def test_report_with_same_name_created_with_different_code_gets_named_different(self):
        report = Report("repeated_report", "some random code",)
        repeated_report = Report("repeated_report", "other random code")
        
        self.assertNotEqual(report.name, repeated_report.name)

    def test_report_with_same_name_and_code_as_another_report_returns_same_instance(self):
        report = Report("repeated_report", "same random code")
        repeated_report = Report("repeated_report", "same random code")
        
        self.assertEqual(report.name, repeated_report.name)

if __name__ == '__main__':
    unittest.main()
