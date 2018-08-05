#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
readme = open(os.path.join(here, 'README.rst')).read()

about = {}
path_to_about = os.path.join(here, "middleman", "__about__.py")
with open(path_to_about) as file:
    # Assign a dictionary representing the __about__'s global symbol table to 
    # about. After that about looks like {'title' : 'middleman', ...}
    exec(file.read(), about) 
    

setup(
    name=about["__title__"],
    version=about["__version__"],
    packages=find_packages(),
    python_requires=">=3.0, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*",
    zip_safe=False,
    include_package_data=True,
    
    # metadata to display on PyPI
    description=about["__summary__"],
    long_description=readme,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: End Users/Desktop",
        "Operating System :: POSIX :: Linux",
        "Topic: :: Internet :: Proxy Servers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    license=about["__license__"],
    url=about["__url__"],
    author=about["__author__"],
    author_email=about["__email__"],
    
    entry_points={
        'console_scripts' : [
        'middleman = middleman.server:main',
        ]
    }
)