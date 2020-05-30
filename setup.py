from setuptools import setup, find_packages

setup(
    name='camunda-external-task-client-python',
    version='1.1.1',
    author='Deserve Labs',
    author_email='devteam@deserve.com',
    packages=find_packages(exclude=("tests",)),
    url='https://github.com/trustfactors/camunda-external-task-client-python',
    license='LICENSE.txt',
    description='Camunda Exteranl Task Client for Python',
    long_description=open('README.md').read(),
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)
