#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

pwd = lambda f: path.join(path.abspath(path.dirname(__file__)), f)
contents = lambda f: open(pwd(f)).read().strip()


setup(
    name='meduza',
    description = 'A python library for meduza',
    version='0.1',
    author='DoAT Media LTD.',
    author_email='dvirsky@gmail.com',
    url='https://github.com/EverythingMe/meduza-py',
    packages=find_packages(),
    install_requires=['redis>=2.10', 'pymongo>=2.8','hiredis>=0.1.6', 'pyyaml', 'requests'],
)
