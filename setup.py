import sys
from setuptools import setup, find_packages


if sys.version_info[:2] < (2, 6):
    raise RuntimeError('Requires Python 2.6 or better')

VERSION = '0.1.0'

install_requires = [
    'pyyaml',
    'ujson',
]

tests_require = install_requires + ['nose']

setup(
    name='hustle',
    version=VERSION,
    description=('Hustle: a data warehouse system.'),
    keywords='hustle',
    author='Chango Inc.',
    author_email='dev@chango.com',
    url='http://chango.com',
    license='MIT License',
    packages=find_packages(exclude=['test', 'deps', 'examples', 'inferno']),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite='nose.collector',
    requires=['disco'])
