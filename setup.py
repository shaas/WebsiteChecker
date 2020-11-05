# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='WebsiteChecker',
    version='0.1.0',
    description='Sample package for using Kafka',
    long_description=readme,
    author='Stefan Haas',
    author_email='haas273@gmail.com',
    url='https://github.com/shaas/WebsiteChecker',
    license=license,
    packages=find_packages(exclude=('tests'))
)
