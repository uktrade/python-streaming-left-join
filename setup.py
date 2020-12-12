import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='streaming-left-join',
    version='0.0.2',
    author='Department for International Trade',
    author_email='webops@digital.trade.gov.uk',
    description='Join iterables in code without loading them all in memory: similar to a SQL left join.',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/python-streaming-left-join',
    py_modules=[
        'streaming_left_join',
    ],
    python_requires='>=3.6.3',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ]
)
