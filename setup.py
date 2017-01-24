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


setup(
    name=PACKAGE_NAME,
    version=get_version(),
    author='Mathias Fröjdman',
    author_email='mwf@iki.fi',
    url='https://github.com/mwfrojdman/ammoo',
    description='AMQP library with a pythonic API for asyncio on Python 3.5+',
    # TODO: download_url,
    packages=PACKAGES,
    package_data={'': ['LICENSE']},
    tests_require=['pytest', 'pytest-asyncio', 'pytest-timeout', 'pytest-capturelog'],
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    platforms='all'
)
