from setuptools import setup


setup(
    name='pipeline_lib',
    version='0',
    packages=['pipeline'],
    requires=['pyspark', 'boto3'],
    license_files = ('LICENSE.txt',),
)
