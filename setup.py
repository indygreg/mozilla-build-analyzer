# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

try:
    from setuptools import setup
except:
    from distutils.core import setup

setup(
    name='mozbuilddata',
    version='0.0.1',
    author='Gregory Szorc',
    author_email='gps@mozilla.com',
    description='Fetch, store, and analyze information about Mozilla builds.',
    license='MPL',
    packages=['mozbuilddata'],
    install_requires=[
        'cql=1.4.0',
        'mach=0.2',
        'pytz>=2013b',
        'urllib3=1.6',
    ],
    scripts=['bin/mbd'],
)

