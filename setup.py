from setuptools import setup, find_packages

execfile('openprocurement/search/version.py')

setup(
    name='openprocurement.search',
    version=__version__, # NOQA
    description="OpenProcurement search service with index_worker",
    long_description=open("README.md").read(),
    # Get more strings from
    # http://pypi.python.org/pypi?:action=list_classifiers
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
    ],
    keywords='openprocurement procurement prozorro search',
    author='Volodymyr Flonts',
    author_email='flyonts@gmail.com',
    license='Apache License 2.0',
    url='https://github.com/openprocurement/openprocurement.search',
    namespace_packages=['openprocurement'],
    packages=find_packages(),
    package_data={'': ['*.md', '*.txt', 'openprocurement/search/index/settings/*.json']},
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'elasticsearch==1.9.0',
        'Flask',
        'pytz',
        'iso8601',
        'PyYAML',
        'requests',
        'retrying',
        'simplejson',
        'setuptools',
        'distribute',
    ],
    entry_points={
        'console_scripts': [
            'index_worker = openprocurement.search.index_worker:main',
            'search_server = openprocurement.search.search_server:main',
            'clean_indexes = openprocurement.search.clean_indexes:main',
            'ocds_ftp_sync = openprocurement.search.ocds_ftp_sync:main',
            'test_load = openprocurement.search.test_load:main',
            'test_index = openprocurement.search.test_index:main',
            'test_search = openprocurement.search.test_search:main',
            'update_orgs = openprocurement.search.update_orgs:main',
        ],
        'paste.app_factory': [
            'search_server = openprocurement.search.search_server:make_app'
        ]
    }
)
