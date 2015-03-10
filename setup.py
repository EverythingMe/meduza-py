#!/usr/bin/env python
from setuptools import setup, find_packages
import os

version = 0, 1

def read_requirements():

    name = os.path.join(os.path.dirname(os.path.abspath(__file__)),'requirements.txt')
    
    with open(name) as fp:
        return filter(None, fp.read().splitlines())


setup(
    name='meduza',
    description = 'A python library for meduza',
    version='{0[0]}.{0[1]}.{1}'.format(version, os.getenv('BUILD_NUMBER', 0)),
    author='DoAT Media LTD.',
    author_email='dvir@everything.me',
    url='https://gitlab.doit9.com/backend/meduza-py',
    packages=find_packages(),
    install_requires=read_requirements()

)

