#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

import haystack_elasticsearch

with open('requirements.txt', 'r') as f:
    requires = f.read().splitlines()


class Tox(TestCommand):
    user_options = [('tox-args=', 'a', "Arguments to pass to tox")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = ''

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import tox
        import shlex
        errno = tox.cmdline(args=shlex.split(self.tox_args))
        sys.exit(errno)


setup(
    name='ebury-elastic',
    version=haystack_elasticsearch.__version__,
    description=haystack_elasticsearch.__description__,
    long_description='\n'.join([open('README.rst').read(), open('CHANGELOG').read()]),
    author=haystack_elasticsearch.__author__,
    author_email=haystack_elasticsearch.__email__,
    url=haystack_elasticsearch.__url__,
    packages=[
        'haystack_elasticsearch',
    ],
    include_package_data=True,
    install_requires=requires,
    license=haystack_elasticsearch.__license__,
    zip_safe=False,
    keywords='python, django, search, index, haystack, elasticsearch',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    test_suite='tests',
    tests_require=['tox'],
    cmdclass={'test': Tox},
)

