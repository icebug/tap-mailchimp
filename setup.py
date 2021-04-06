#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(
    name="tap-mailchimp",
    version="0.0.1",
    description="Singer.io tap for extracting data from the Mailchimp API",
    author="Maria Larsson",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_mailchimp"],
    install_requires=[
        "tap-framework==0.0.4",
    ],
    entry_points="""
          [console_scripts]
          tap-mailchimp=tap_mailchimp:main
      """,
    packages=find_packages(),
    package_data={"tap_mailchimp": ["schemas/*.json"]},
)
