import sys
import logging

def get_local_hadoop():
    classpaths = [
        os.path.join(os.path.dirname(__file__), "external", "pig-0.2.0", "pig.jar"),
        os.path.join(os.path.dirname(__file__), "external", "pig-0.2.0", "lib", "*"),
    ]
    local_home = os.path.join(os.path.dirname(__file__), "external", "hadoop-0.18.3")
    name_node = "file:///"

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
hadoop = get_local_hadoop()
hadoop.grunt_shell()
