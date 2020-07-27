# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2014.                                      |
# +--------------------------------------------------------------------------+
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi, Steven James|
# +--------------------------------------------------------------------------+

import sys

from setuptools import setup, find_packages, find_namespace_packages
from distutils.core import setup, Extension

PACKAGE = 'django-pyodbc-iseries'
# VERSION = __import__('django_iseries').__version__
VERSION = '1.0.0'
LICENSE = 'Apache License 2.0'
# DESCRIPTION = __import__('django_iseries').__doc__
DESCRIPTION = ''

print(find_packages(where='src'))

setup(
    name=PACKAGE,
    version=VERSION,
    license=LICENSE,
    platforms='All',
    install_requires=['pyodbc>=4.0.27', 'django>=2.2.0', 'pytz', 'sqlparse'],
    dependency_links=['https://pypi.org/project/pyodbc/', 'http://pypi.python.org/pypi/Django/',
                      'https://www.ibm.com/support/pages/ibm-i-access-client-solutions'],
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    author='Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi, Steven James',
    author_email='steven@waitforitjames.com',
    maintainer='Steven James',
    maintainer_email='steven@waitforitjames.com',
    url='https://github.com/soundstripe/django-pyodbc-iseries',
    keywords='django iseries backends adapter IBM Data Servers database db2',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    classifiers=['Development Status :: 4 - Beta',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: Apache Software License',
                 'Operating System :: Microsoft :: Windows :: Windows NT/2000',
                 'Operating System :: Unix',
                 'Operating System :: POSIX :: Linux',
                 'Operating System :: MacOS',
                 'Topic :: Database :: Front-Ends'],
    data_files=[('', ['./README.md']),
                ('', ['./CHANGES']),
                ('', ['./LICENSE'])],
    zip_safe=False,
    include_package_data=True,
)
