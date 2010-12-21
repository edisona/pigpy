import os
from distutils.core import setup

setup(
    name="pigpy",
    packages=['pigpy'],
    version="0.6",
    license="MIT",
    url="http://pypi.python.org/pypi/pigpy",
    author="Marshall Weir",
    author_email="marshall.weir+pigpy@gmail.com",

    classifiers=[
        "Programming Language :: Python",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Natural Language :: English",
        "Topic :: Software Development",
    ],
    description="a python tool to manage Pig reports",
    long_description=open(os.path.join(os.path.dirname(__file__), "README.txt")).read()
)
