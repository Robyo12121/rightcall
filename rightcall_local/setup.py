from setuptools import setup

setup(
    name='rightcall_local',
    version='0.1',
    py_modules=['cli.py'],
    install_requires=[
        'Click',
        ],
    entry_points='''
        [console_scripts]
        
