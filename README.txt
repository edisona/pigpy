pypig - a python tool to manage Pig reports

Pig provides an amazing set of tools to create complex relational processes on top of Hadoop, but it has a few missing pieces:
    1) Looping constructs for easily creating multiple similar reports
    2) Caching of intermediate calculations
    3) Data management and cleanup code
    4) Easy testing for report correctness

pypig is an attempt to fill in these holes by providing a python module that knows how to talk to a Hadoop cluster and can create and manage complex report structures.

h1. Getting started

h2. Installation

Currently there is no real install procedure. Checkout the current trunk and play with it from there. The only requirement I am aware of is that the java executable must be on your path and must be 1.6 or newer. The grunt.py file at the top level should be a reasonable example of where to start using this module, in addition to this tutorial. pigpy has been tested on OS X and Debian Linux.

h2. Submitting reports
At its basic level, pypig is a tool to submit Pig jobs to Hadoop and push and pull data on a cluster. Assume we have a pig report called report.pig in the current directory.

<code>
from bacon.hadoop import Hadoop

classpaths = [
    os.path.join(os.path.dirname(__file__), "external", "pig-0.2.0", "pig.jar"),
    os.path.join(os.path.dirname(__file__), "external", "pig-0.2.0", "lib", "*"),
]
local_home = os.path.join(os.path.dirname(__file__), "external", "hadoop-0.18.3")
name_node = "file:///"

hadoop = Hadoop(local_home, name_node, classpaths)
hadoop.run_pig_job("report.pig")
hadoop.copyToLocal("/reports/output.csv", "output.csv")
</code>

This code runs the report on whatever cluster is specified by name_node, and then pulls the output (assuming the output path is /reports/output.csv) to the local filesystem. The name_node argument can be any HDFS url (eg. hdfs://hadoop_cluster_ip:54310). Test code can be easily written to point to data on the local filesystem or a test cluster to make verifying correct results easier.

h2. Creating complex reports

Beyond just submitting reports, pypig provides tools to create very complex reports using Python and Pig Latin rather than just Pig Latin. This allows you to use the higher level python constructs to generate pig code instead of maintaining large reports by hand.

Below is an example of a very simple report providing statistics on cars by color.

<code>
from bacon.hadoop import Hadoop
from bacon.reports import Report, Plan, PlanRunner

hadoop = (hadoop initialization code)

data = Report("car_color_data",
    "LOAD 'car_colors.tsv' AS (make, model, price, color);"
)
color_reports = []
for price in [5000, 10000, 15000, 20000, 150000]:
    color_reports.append(Report("%s_cars" % price,
        parents={"color_data": data}
        code="""%(this)s = FILTER %(color_data)s BY price < """ + str(price) + """;
                %(this)s = FOREACH (GROUP %(this)s BY color) {
                    GENERATE group.color, AVG(%(this)s.price); 
                }"""))

plan = Plan("/tmp/reports", color_reports)
print plan.pigfile

plan_runner = PlanRunner(plan, hadoop)
plan_runner.run_reports()
plan_runner.save_reports("~/reports")
plan_runner.cleanup()
</code>

This will print the following pigfile and then run it and save the results to the local machine:

<code>
LOAD 'car_colors.tsv' AS (make, model, price, color);
5000_cars = FILTER car_color_data BY price < 5000;
                5000_cars = FOREACH (GROUP 5000_cars BY color) {
                    GENERATE group.color, AVG(5000_cars.price); 
                }

STORE 5000_cars INTO '/tmp/reports/1244375262.84/5000_cars' USING PigStorage(',');
10000_cars = FILTER car_color_data BY price < 10000;
                10000_cars = FOREACH (GROUP 10000_cars BY color) {
                    GENERATE group.color, AVG(10000_cars.price); 
                }

STORE 10000_cars INTO '/tmp/reports/1244375262.84/10000_cars' USING PigStorage(',');
15000_cars = FILTER car_color_data BY price < 15000;
                15000_cars = FOREACH (GROUP 15000_cars BY color) {
                    GENERATE group.color, AVG(15000_cars.price); 
                }

STORE 15000_cars INTO '/tmp/reports/1244375262.84/15000_cars' USING PigStorage(',');
20000_cars = FILTER car_color_data BY price < 20000;
                20000_cars = FOREACH (GROUP 20000_cars BY color) {
                    GENERATE group.color, AVG(20000_cars.price); 
                }

STORE 20000_cars INTO '/tmp/reports/1244375262.84/20000_cars' USING PigStorage(',');
150000_cars = FILTER car_color_data BY price < 150000;
                150000_cars = FOREACH (GROUP 150000_cars BY color) {
                    GENERATE group.color, AVG(150000_cars.price); 
                }

STORE 150000_cars INTO '/tmp/reports/1244375262.84/150000_cars' USING PigStorage(',');
</code>

While this example is trivial to do in raw Pig Latin (and can be written much better with a clever GROUP ... BY), even here it is much easier to alter the results for each report in the python wrapper than in the resulting Pig Latin. Additionally, if I had been lazy and not renamed each alias, instead calling each report car_price, the module will helpfully rename each alias car_price, car_price_1, car_price_2, etc. Here, it would not make any difference, as we are immediately writing each data bag to the filesystem and not doing further processing.

This renaming property is why I use %(this)s in the report template in the loop. When the Report object is written to the pig file, self and the parents are filled in by dictionary string interpolation. %(this)s is a special key used to pull in the actual name of the current report. There is no requirement to use any of this functionality, the report could just be the result code and avoid using the loop or parents, but this makes it much harder to alter reports in the future.

The paths on the hadoop filesystem are pretty horrible to access directly, which is why the PlanRunner can deal with saving them to the local directory. The folder name is just the timestamp to try to avoid having Pig complain if the folder already exists. The save_reports method will pull things to a user specified directory from whatever horrible path is used in Hadoop, and the cleanup method on the PlanRunner will remove all traces of the Pig job from the HDFS.

h2. Caching intermediate results

The following code generates 3 reports to save, all based on the same initial results. For the purpose of this example, assume the custom UDF takes hours to process the files and each final report takes a few minutes.

<code>
from pigpy.hadoop import Hadoop
from pigpy.reports import Report, Plan, PlanRunner

hadoop = (hadoop initialization code)

slow_report = Report("demographic_aggregate",
    code="""%(this)s = LOAD 'us_census.tsv' AS (age, wage, family_size, state);
%(this)s = (horrible filtering/grouping custom UDF) AS (age, wage, family_size, state, count);"""
)

state_reports = []
for state in ["michigan", "ohio", "nevada"]:
    state_reports.append(Report(state, parents={"demo": slow_report}, 
        code="%%(this)s = FILTER %%(demo)s BY state == '%s';" % state))

plan = Plan("/tmp/reports", state_reports)
print plan.pigfile

plan_runner = PlanRunner(plan, hadoop)
plan_runner.run_reports()
plan_runner.save_reports("~/reports")
plan_runner.cleanup()
</code>

This code will generate and run the following Pig report:

<code>
demographic_aggregate = LOAD 'us_census.tsv' AS (age, wage, family_size, state);
demographic_aggregate = (horrible filtering/grouping custom UDF) AS (age, wage, family_size, state, count);
michigan = FILTER demographic_aggregate BY state == 'michigan';

STORE michigan INTO '/tmp/reports/1244588320.16/michigan' USING PigStorage(',');
ohio = FILTER demographic_aggregate BY state == 'ohio';

STORE ohio INTO '/tmp/reports/1244588320.16/ohio' USING PigStorage(',');
nevada = FILTER demographic_aggregate BY state == 'nevada';

STORE nevada INTO '/tmp/reports/1244588320.16/nevada' USING PigStorage(',');
</code>

With the current version of Pig (0.2.0 at the time of this tutorial), this Pig job will re-run the demographic_aggregate for each of the 3 subsidiary reports. Hopefully, this will be corrected in a future release of Pig, but until then, pigpy can force caching of these results for you. If you set up a logging handler for pigpy.reports, you should see the following warning:

WARNING:pigpy.reports:Report demographic_aggregate should be cached, but does not have cache_columns

This means pigpy has found a report it thinks should be cached, but cannot find appropriate hints to save and load it. If we modify the demographic_aggregate to this:

<code>
slow_report = Report("demographic_aggregate",
    code="""%(this)s = LOAD 'us_census.tsv' AS (age, wage, family_size, state);
%(this)s = (horrible filtering/grouping custom UDF) AS (age, wage, family_size, state, count);""",
    cache_columns="age, wage, family_size, state, count"
)
</code>

pigpy will add these two lines to the Pig report after the demographic_aggregate has run its horrible calculation:

<code>
STORE demographic_aggregate INTO '/tmp/reports/1244589140.23/demographic_aggregate' USING PigStorage(',');
demographic_aggregate = LOAD '/tmp/reports/1244589140.23/demographic_aggregate' USING PigStorage(',') AS (age, wage, family_size, state, count);
</code>

Pig will see this code and save the results for demographic_aggregate to file. When future aliases use demographic_aggregate, they will iterate back up through the report and find the load line and load the previous results rather than running the calculations again. If you use caching extensively, it is very important to make sure the cleanup method gets called, or your HDFS will fill up very quickly. Additionally, pigpy is not very smart about the caching, so it will force caching every time you supply cache_columns and there is more than one subsidiary report. This may cause additional map-reduce jobs to get run on your cluster.

At Zattoo, caching intermediate values has been improved performance up to 10x depending on the number of subsidiary reports.
