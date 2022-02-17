#
# Copyright 2021- University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

""" Installation script for the deriva package.
"""

from setuptools import setup, find_packages
import re
import io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('deriva/chisel/__init__.py', encoding='utf_8_sig').read()
    ).group(1)

url = "https://github.com/informatics-isi-edu/chisel/"
author = 'USC Information Sciences Institute, Informatics Systems Research Division'
author_email = 'isrd-support@isi.edu'

setup(
    name="deriva-chisel",
    version=__version__,
    description="CHiSEL: schema evolution and model management for the DERIVA platform.",
    long_description='For further information, visit the project [homepage](%s).' % url,
    long_description_content_type='text/markdown',
    url=url,
    author=author,
    author_email=author_email,
    maintainer=author,
    maintainer_email=author_email,
    packages=find_packages(),
    package_data={},
    test_suite='tests',
    install_requires=[
        'deriva>=1.5.0',
        'graphviz',
        'nltk',
        'pyfpm',
        'pyparsing',
        'rdflib'
    ],
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ]
)
