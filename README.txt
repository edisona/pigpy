pypig - a python tool to manage Pig reports

Pig provides an amazing set of tools to create complex relational processes on top of Hadoop, but it has a few missing pieces:
    1) Looping constructs for easily creating multiple similar reports
    2) Caching of intermediate calculations
    3) Data management and cleanup code
    4) Easy testing for report correctness

pypig is an attempt to fill in these holes by providing a python module that knows how to talk to a Hadoop cluster and can create and manage complex report structures.

h1. Getting started

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



