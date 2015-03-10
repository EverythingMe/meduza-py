#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='meduza',
    description = 'A python library for meduza',
    version='0.1',
    author='DoAT Media LTD.',
    author_email='dvir@everything.me',
    url='https://gitlab.doit9.com/backend/meduza-py',
    packages=find_packages(),
    install_requires=filter(None, open('requirements.txt').read().splitlines())

)

