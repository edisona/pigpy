import sys
sys.path.append("..")
import os
import unittest
import re
import logging
import shutil
from time import time

class Report(object):
    #TODO: figure out more elegant way to manage this. global state=yuck
    __reports = {}
    
    @classmethod
    def reset_reports(klass):
        klass.__reports = {}
    
    @classmethod
    def __register_report(klass, report):
        klass.__reports[report.name] = report

    @classmethod
    def __build_code(klass, name, raw_code, parents):
        #Create the code for this report
        dictparams = dict(**parents)
        dictparams["this"] = name
        return raw_code % dictparams

    @classmethod
    def __correct_name_for_report(klass, name, raw_code, parents):
        subscript = 1
        test_name = name
        while test_name in klass.__reports:
            registered_report = klass.__reports[name]
            
            #Check to see if the code and the parents are identical
            if (registered_report.code == klass.__build_code(name, raw_code, parents)
                and sorted(registered_report.parents) == 
                    [p for p in sorted(parents.itervalues())]):
                return test_name #These are in fact the same report
            
            test_name = "%s_%s" % (name, subscript)
            subscript += 1
        #No report with the same name
        return test_name
    
    def __init__(self, name, code, parents=None, cache_columns=None, human_readable_columns=None):
        self.__cache = False
        self.__children = []
        
        self.__parents = parents or {}
        self.__cache_columns = cache_columns or None
        self.__human_columns = human_readable_columns or None

        self.__raw_code = code
        #uniquify creates the report code and makes sure name is unique across all reports
        #As well as registering the report
        self.__uniquify_report(name)
        
    def __uniquify_report(self, name):
        self.__name = self.__class__.__correct_name_for_report(
            name, 
            self.__raw_code,
            self.__parents
        )
        self.__code = self.__class__.__build_code(
            self.__name, 
            self.__raw_code,
            self.__parents
        )
        
        if self.name in self.__class__.__reports:
            #This report is a duplicate of another, make them the same
            self = self.__class__.__reports[self.name]
        else:
            self.__class__.__register_report(self)

    def request_caching(self):
        self.__cache = True

    def __get_human_columns(self):
        """Provide the best possible human readable column names. may be None"""
        if self.__human_columns is not None:
            return self.__human_columns
        else:
            return self.cache_columns

    def __str__(self):
        return self.__name
        
    needs_cache = property(lambda self: self.__cache)
    cache_columns = property(lambda self: self.__cache_columns)
    human_readable_columns = property(__get_human_columns)
    code = property(lambda self: self.__code)
    name = property(lambda self: self.__name)
    parents = property(lambda self: [parent for parent in self.__parents.itervalues()])
    
class Plan(object):
    def __init__(self, output_directory="/tmp", reports=None, save_format="USING PigStorage(',')"):
        #TODO: create utility function that give back appropriate handlers
        #logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
        self.log = logging.getLogger(__name__)
        self.__reports = reports or []
        self.__save_format = save_format
        self.output_directory =  os.path.join(output_directory, str(time()))
        
        
    def add(self, report):
        self.__reports.append(report)
        
    def __save_or_cache_report(self, report, pigfile):
        if report in self.__reports or (report.needs_cache and report.cache_columns):
            report_path = os.path.join(self.output_directory, report.name)
            
            pigfile += "\nSTORE %(report)s INTO '%(report_path)s' %(save_format)s;\n" % {
                    "report": report,
                    "report_path": report_path,
                    "save_format": self.__save_format,
            }

        if report.needs_cache and report.cache_columns:
            pigfile += "%(report)s = LOAD '%(report_path)s' %(save_format)s AS (%(columns)s);\n" % {
                "report": report, 
                "report_path": report_path,
                "columns": report.cache_columns,
                "save_format": self.__save_format,
            }

        if report.needs_cache and not report.cache_columns:
            self.log.warning("Report %s should be cached, but does not have cache_columns" % report)

        return pigfile
        
    def __get_pigfile(self):
        #Create sorted list of statement dependencies
        sorted_reports = []
        #Get to cheat a little - this should proabbly be topographical sort
        #But we just have to make sure dependencies are run first
        #TODO:remove deps as an argument. My recursive brain appears to be absent today.
        def add_sorted_deps(reports, deps):
            for report in reports:
                if report not in deps:
                    self.log.debug("Adding parents for %s" % report)
                    add_sorted_deps(report.parents, deps)
                    self.log.debug("Adding report %s" % report)
                    deps.append(report)
                else:
                    #We need this report more than once, so cache teh results
                    self.log.debug("Cacheing report %s" % report)
                    report.request_caching()
            
        add_sorted_deps(self.__reports, sorted_reports)
        
        pigfile = ""
        for report in sorted_reports:
            pigfile += report.code + "\n"
            pigfile = self.__save_or_cache_report(report, pigfile)
            
        return pigfile
    
    pigfile = property(__get_pigfile)
    reports = property(lambda self: self.__reports)


class PlanRunner(object):
    def __init__(self, plan, hadoop_wrapper):
        self._plan = plan
        self.__hadoop = hadoop_wrapper

    pigfile = property(lambda self: self._plan.pigfile)

    def run_reports(self, cleanup=True):
        #Create the report file
        report_filename = os.tempnam("/tmp")
        open(report_filename, "w").write(self.pigfile)

        try:
            #submit the job to Hadoop
            self.__hadoop.run_pig_job(report_filename)
        finally:
            #Remove the report file
            if cleanup:
                if os.path.exists(report_filename):
                    os.remove(report_filename)

    def save_reports(self, folder, header_lookup={}):
        if os.path.exists(folder):
            #We want to move the old folder if it exists
            if folder.endswith("/"):
                folder = folder[:-1]
            shutil.move(folder, "%s-%s.old" % (folder, str(time())))
        os.makedirs(folder)
        for report in self._plan.reports:
            name = report.name
            header = report.human_readable_columns
            if name in header_lookup:
                #Always use provided headers if possible
                header = ",".join(header_lookup[name])
            #pull the reports out of Hadoop
            local_path = os.path.join(folder, name)
            remote_path = os.path.join(self._plan.output_directory, name)
            self.__hadoop.copy_pig_report_to_file(remote_path, local_path, header=header)

    def cleanup(self):
        """Clean up data in the HDFS"""
        self.__hadoop.rmr(self._plan.output_directory)



