#!/usr/bin/env python

from setuptools import setup

setup(
    name='target-blob-storage',
    version='1.0.0',
    description='hotglue target for exporting data to Azure Blob Storage',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_blob_storage'],
    install_requires=[
        'azure-storage-blob==12.8.1',
        'argparse==1.4.0'
    ],
    entry_points='''
        [console_scripts]
        target-blob-storage=target_blob_storage:main
    ''',
    packages=['target_blob_storage']
)
