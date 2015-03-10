#!/usr/bin/env python
from setuptools import setup, find_packages
import os

version = 0, 1

setup(
    name='meduza',
    description = 'A python library for meduza',
    version='{0[0]}.{0[1]}.{1}'.format(version, os.getenv('BUILD_NUMBER', 0)),
    author='DoAT Media LTD.',
    author_email='dvir@everything.me',
    url='https://gitlab.doit9.com/backend/meduza-py',
    packages=find_packages(),
    install_requires=filter(None, open('requirements.txt').read().splitlines())

)

