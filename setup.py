# -*- encoding: utf-8 -*-
import re
import os

from setuptools import setup

PACKAGE_NAME = 'ammoo'
PACKAGES = [
    'ammoo',
    'ammoo.auth',
    'ammoo.exceptions',
    'ammoo.wire',
    'ammoo.auth.mechanisms',
    'ammoo.wire.frames',
    'ammoo.wire.low',
    'ammoo.wire.frames.method'
]


def get_version():
    version_py_path = os.path.join(os.path.dirname(__file__), PACKAGE_NAME, 'version.py')
    pattern = re.compile(r'''^__version__\s*=\s*(?P<quote>['"])(?P<version>[^'"]+)(?P=quote)\s*(?:#.*)?$''')
    with open(version_py_path, 'r', encoding='utf-8') as fob:
        for line in fob:
            match = pattern.match(line)
            if match is not None:
                return match.group('version')
    raise ValueError('No version found')


VERSION = get_version()


setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author='Mathias Fröjdman',
    author_email='mwf@iki.fi',
    url='https://github.com/mwfrojdman/ammoo',
    description='AMQP library with a pythonic API for asyncio on Python 3.5+',
    download_url='https://github.com/mwfrojdman/ammoo/archive/{}.tar.gz'.format(VERSION),
    packages=PACKAGES,
    package_data={'': ['LICENSE']},
    extras_require={
        'test': [
            'pytest',
            'pytest-asyncio',
            'pytest-timeout',
            'pytest-capturelog',
        ],
    },
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
	'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    platforms='all'
)
