from setuptools import setup, find_packages

setup(
    name='camunda-external-task-client-python',
    version='0.1.0',
    author='Deserve Labs',
    author_email='devteam@deserve.com',
    packages=find_packages(),
    url='https://github.com/trustfactors/camunda-external-task-client-python',
    license='LICENSE.txt',
    description='Camunda Exteranl Task Client for Python',
    long_description=open('README.md').read(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
    ],
)
