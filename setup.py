
import os
import json
from os import sep as os_sep
from setuptools import setup, find_packages


# canonical python package is structured as
# package_dir_name/
#   -- setup.py
#   -- package_dir_name/
# get top package_dir_name and know it will contain the same
__pack_dir = os.path.split(os.path.dirname(os.path.abspath(__file__)))[1]
with open('{0}{1}pkg_info.json'.format(__pack_dir, os_sep), 'r') as f:
    PKG_INFO = json.loads(f.read())


# 2.7 patch for importing package_data unicode_literal bug in setuptools
# fill in package data up here and it will be forced to str below
__pgk_data = {
    PKG_INFO['name']: [
        'pkg_info.json',
        'cfg/*.ini'
    ]
}


setup(
    name=PKG_INFO['name'],

    version=PKG_INFO['version'],

    description=PKG_INFO['description'],
    long_description=open('README.rst').read(),

    packages=find_packages(),

    include_package_data=True,
    package_data={
        str(k): [ str(vi) for vi in v ]
        for k, v in __pgk_data.items()
    },

    scripts=[

    ],

    platforms='any',

    zip_safe=False,

    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Operating System :: OS Independent'
    ],

    test_suite='tests',
)
