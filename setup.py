from setuptools import setup, find_packages
from os.path import join, dirname

setup(
    name='project_search',
    version='1.0.0',
    packages=find_packages(),
    long_description=open(join(dirname(__file__), 'README.md')).read(),
    install_requires=[
        'Cython>=0.29.15',
        'spacy',
        'enry>=0.1.1',
        'Pygments>=2.10.0',
        'tree_sitter>=0.2.1'
        'click>=8.0.4',
        'elasticsearch>=7.15.2'
    ]
)
