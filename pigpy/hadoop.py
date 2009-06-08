"""Module to interface with the Hadoop filesystem"""

import sys
import os
import subprocess
import logging
import tempfile
import shutil

class HDFSError(StandardError):
    pass
    
class PigError(StandardError):
    pass

class Hadoop(object):
    
    #Return true if this will be running on a cluster on localhost
    local_mode = property(lambda self: "file:///" in self.__local_home)
    
    def __init__(self, local_home, name_node, pig_classpaths):
        self.log = logging.getLogger(__name__)
        self.__name_node = name_node
        #We can assume all paths are relative to this file
        #If an absolute path is specified, os.path.join will discard
        #all previous path components
        abs_path = os.path.abspath(os.path.dirname(__file__))
        self.__local_home = os.path.join(abs_path, local_home)
        self.__pig_classpaths = [os.path.join(abs_path, path) for path in pig_classpaths]

    def __send_to_command_line(self, command):
        self.log.debug(command)
        result = subprocess.Popen(command)
        result.wait()
        return result.returncode


#HDFS Commands

    def run_hdfs_command(self, hdfs_command, *args):
        """All other hdfs routines should be implemented around this function
    
        Eventually, we will probably want to wrap libhdfs.so using SWIG
        """

        common_prefix = [
            os.path.join(self.__local_home, "bin", "hadoop"), #The hadoop binary
            "dfs",
            "-fs", #fs specifies the hadoop filesystem to connect to
            self.__name_node
        ]
        command = common_prefix + [hdfs_command] + list(args)
        
        returncode = self.__send_to_command_line(command)
        if returncode != 0:
            raise HDFSError("hdfs command returned error code %s" % returncode)
    
    def copyFromLocal(self, src, dest):
        """Find src on the local filesystem and copy it to dest on the hdfs"""
        self.run_hdfs_command("-copyFromLocal", src, dest)
    
    def copyToLocal(self, src, dest):
        """Find src on the hdfs and copy it to dest on the local filesystem"""
        self.run_hdfs_command("-copyToLocal", src, dest)
    
    def test(self, path, isDirectory=False):
        """Return true if there is a folder or file at path in the HDFS."""
        try:
            if isDirectory:
                #Return true if the path is a directory
                self.run_hdfs_command("-test", "-d", path)
            else:
                #Return true if the path exists
                self.run_hdfs_command("-test", "-e", path)
            return False;
        except HDFSError, e:
            #If test had a non-zero return code, the path exists
            return True
            
    def rm(self, path):
        """Remove the file at path on the HDFS"""
        self.run_hdfs_command("-rm", path)
        
    def rmr(self, path):
        """Remove the folder tree at path on the HDFS"""
        self.run_hdfs_command("-rmr", path)

#Pig Commands

    def copy_pig_report_to_file(self, report_path, local_filename, header=None):
        """Copies a pig report from the HDFS to local_filename.
        
        First all output files for the pig reports are pulled from HDFS, then they
        get concatenated at the final location. An optional header can be supplied.
        While this can be any string, the purpose is to allow column headers for the report.
        """
        
        #First we get all the part files to a temp directory
        temp_directory = tempfile.mkdtemp(dir="/tmp")
        if self.remote_path_exists(report_path, isDirectory=True):
            self.copyToLocal(os.path.join(report_path, "part*"), temp_directory)
        else:
            self.copyToLocal(report_path, temp_directory)
    
        #Now we read all files in the temp directory and smash them together to make the final results
        local_file = open(local_filename, "w")
        if header is not None:
            local_file.write(header + "\n")
        for filename in os.listdir(temp_directory):
            for line in open(os.path.join(temp_directory, filename)):
                local_file.write(line)
        
        print >> sys.stderr, "Wrote pig report to %s" % local_filename
        self.log.debug("Wrote pig report to %s" % local_filename)
        
        #finally, clean up after ourselves (rmtree doesn't care if the dir is empty)
        if os.path.exists(temp_directory):
            shutil.rmtree(temp_directory)
        else:
            self.log.info("Could not find %s for removal." % temp_directory)

    def get_classpaths(self):
        """Return all paths java will need to run pig."""
        classpath = ":".join(self.__pig_classpaths)
        
        #file is local mode, where we do not need the hadoo config
        if not self.local_mode:
            #Pig also expects the hadoop conf directory on the classpaths
            hadoop_conf = os.path.join(self.__local_home, "conf")
            classpath = ":".join([classpath, hadoop_conf])

        return classpath
        
    def grunt_shell(self):
        """Run the grunt shell interactively"""
        self.run_pig_job("")

    def run_pig_job(self, filename):
        """Submit a report to the hadoop cluster to be run
    
        This will wait until the pig job is finished running, which may be some time.
        """
        command = [
            "java",
            #It seems submitting pig scripts can take up a lot of memory
            "-Xmx1024m",
            "-cp",  
            self.get_classpaths(),
            "org.apache.pig.Main"
        ]
        if self.local_mode:
            command += ["-x", "local"]
        if filename is not "":
            command.append(filename)

        returncode = self.__send_to_command_line(command)
        if returncode != 0:
            raise PigError("Pig job had error code %s" % returncode)
