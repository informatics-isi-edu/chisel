"""CHiSEL setup."""

from setuptools import setup, find_packages
import re
import io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('chisel/__init__.py', encoding='utf_8_sig').read()
    ).group(1)

setup(
    name="chisel",
    description="CHiSEL: A high-level, user-oriented framework for schema evolution",
    version=__version__,
    packages=find_packages(),
    package_data={},
    test_suite='tests',
    requires=[
        'deriva',
        'graphviz',
        'nltk',
        'pyfpm',
        'pyparsing',
        'rdflib',
        'requests'
    ],
    install_requires=[
        'setuptools'
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
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)
