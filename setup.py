from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='closeness',

    version='2.0.2',

    description='Mongodb documentand python dict similarity',
    long_description=long_description,

    url='https://github.com/suyati/mongodb-doc-closeness',

    author='Thaha',
    author_email='tkannippoyil@suyati.com',
    maintainer='Suyati Technologies',

    license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],

    keywords='mongodb setuptools development relationship query',

    packages=['closeness'],

)
