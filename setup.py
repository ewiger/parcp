#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' distribute- and pip-enabled setup.py '''
from __future__ import print_function

import sys
import logging
import os
import re
import shutil

# ----- overrides -----

# set these to anything but None to override the automatic defaults
packages = None
package_name = None
package_data = None
scripts = None
# ---------------------


# ----- control flags -----

# fallback to setuptools if distribute isn't found
setup_tools_fallback = True

# don't include subdir named 'tests' in package_data
skip_tests = False

# print some extra debugging info
debug = True

# -------------------------

if debug:
    logging.basicConfig(level=logging.DEBUG)

# distribute import and testing
try:
    import distribute_setup
    distribute_setup.use_setuptools()
    logging.debug("distribute_setup.py imported and used")
except ImportError:
    # fallback to setuptools?
    # distribute_setup.py was not in this directory
    if not (setup_tools_fallback):
        import setuptools
        if not (hasattr(setuptools,'_distribute') and \
                setuptools._distribute):
            raise ImportError("distribute was not found and fallback to setuptools was not allowed")
        else:
            logging.debug("distribute_setup.py not found, defaulted to system distribute")
    else:
        logging.debug("distribute_setup.py not found, defaulting to system setuptools")


import setuptools


def find_scripts():
    return [s for s in setuptools.findall('bin/') if os.path.splitext(s)[1] != '.pyc']


def package_to_path(package):
    """
    Convert a package (as found by setuptools.find_packages)
    e.g. "foo.bar" to usable path
    e.g. "foo/bar"
    No idea if this works on windows
    """
    return package.replace('.','/')


def find_subdirectories(package):
    """
    Get the subdirectories within a package
    This will include resources (non-submodules) and submodules
    """
    try:
        subdirectories = os.walk(package_to_path(package)).next()[1]
    except StopIteration:
        subdirectories = []
    return subdirectories


def subdir_findall(dir, subdir):
    """
    Find all files in a subdirectory and return paths relative to dir
    This is similar to (and uses) setuptools.findall
    However, the paths returned are in the form needed for package_data
    """
    strip_n = len(dir.split('/'))
    path = '/'.join((dir, subdir))
    return ['/'.join(s.split('/')[strip_n:]) for s in setuptools.findall(path)]


def find_package_data(packages):
    """
    For a list of packages, find the package_data
    This function scans the subdirectories of a package and considers all
    non-submodule subdirectories as resources, including them in
    the package_data
    Returns a dictionary suitable for setup(package_data=<result>)
    """
    package_data = {}
    for package in packages:
        package_data[package] = []
        for subdir in find_subdirectories(package):
            if '.'.join((package, subdir)) in packages: # skip submodules
                logging.debug("skipping submodule %s/%s" % (package, subdir))
                continue
            if skip_tests and (subdir == 'tests'): # skip tests
                logging.debug("skipping tests %s/%s" % (package, subdir))
                continue
            package_data[package] += subdir_findall(package_to_path(package), subdir)
    return package_data


def readme():
    try:
        with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
            return f.read()
    except (IOError, OSError):
        return ''


def get_version():
    src_path = os.path.abspath(os.path.dirname(__file__))
    sys.path = [src_path] + sys.path
    from parcp.version import __version__ as parcp_version
    return parcp_version


# ----------- Override defaults here ----------------

scripts = [
    'parcp2.py',
]

packages = [
    'parcp',
]

# package_data = {'': ['*.html', '*.svg', '*.js']}
package_data = {}

if packages is None: packages = setuptools.find_packages('src')

if len(packages) == 0: raise Exception("No valid packages found")

if package_name is None: package_name = packages[0]

if package_data is None: package_data = find_package_data(packages)

if scripts is None: scripts = find_scripts()


setuptools.setup(
    name='parcp',
    version=get_version(),
    description='A library to run CellProfiler in parallel',
    long_description=readme(),
    author='Yauhen Yakimovich',
    author_email='eugeny.yakimovitch@gmail.com',
    url='https://github.com/ewiger/parcp',
    license='MIT',
    platforms=['Linux', 'OS-X'],
    classifiers=[
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Image Recognition',
        'Topic :: Scientific/Engineering :: Information Analysis',
        # 'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        # 'Topic :: System :: Emulators',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python',
        'Programming Language :: Unix Shell',
        # 'Programming Language :: Ruby',
        # 'Programming Language :: Java',
        'Development Status :: 4 - Beta',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
    ],
    scripts=scripts,
    packages=packages,
    # package_dir={'': 'src'},
    package_data=package_data,
    include_package_data=True,
    download_url='https://github.com/ewiger//tarball/master',
    install_requires=[
        'sh>=1.11',
        'PyYAML>=3.11',
    ],
    tests_require=['nose>=1.0'],
    test_suite='nose.collector',
)

