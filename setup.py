#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name='plyvelmp',
    version='0.1',
    description='Support for connections to the same leveldb from different processes',
    
    author='Steep Pepper',
    author_email='steephairy@gmail.com',
    url='https://github.com/longnetwork/plyvelmp',

    python_requires=">=3.11",

    package_dir={
        'plyvelmp': '.',
    },

    packages=['plyvelmp',],

    
    install_requires=[
        'plyvel>=1.5.1',
    ],

)
