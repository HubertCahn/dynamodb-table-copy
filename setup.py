from setuptools import setup, find_packages
from codecs import open
from os import path

__version__ = '0.1.0'

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

# get the dependencies and installs
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if x.startswith('git+')]

setup(
    name='dynamodb_table_copy',
    version=__version__,
    description="Python Script to copy the table on AWS DynamoDB. ",
    long_description=long_description,
    url='https://github.com/HubertCahn/dynamodb-table-copy.git',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database'
    ],
    keywords='aws dynamodb copy-table',
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    author='Hubert Chan',
    install_requires=install_requires,
    dependency_links=dependency_links,
    author_email='hubertchan94@gmail.com'
)