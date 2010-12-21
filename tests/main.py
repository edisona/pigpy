COVERAGE = [
    '--with-coverage',
    '--cover-package=pigpy',
]

FLAGS = ["-v"] + COVERAGE

if __name__ == "__main__":

    import sys
    import os
    import nose

    #Make sure the dev code gets found first for testing
    sys.path.insert( 0, os.path.join( os.path.dirname(__file__), ".." ) )
    nose.core.TestProgram(defaultTest = "tests", argv = (sys.argv + FLAGS))