from setuptools import setup, find_packages

version = '0.6a1'

setup(name='openprocurement.search',
        version=version,
        description="OpenProcurement search service",
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
        packages=find_packages(),
        namespace_packages=['openprocurement'],
        include_package_data=True,
        zip_safe=False,
        install_requires=[
            'elasticsearch==1.9.0',
            'openprocurement_client',
            'Flask',
            'iso8601',
            'PyYAML',
            'requests',
            'retrying',
            'simplejson',
        ],
        entry_points={
            'console_scripts': [
                'index_worker = openprocurement.search.index_worker:main',
                'search_server = openprocurement.search.search_server:main',
                'clean_indexes = openprocurement.search.clean_indexes:main',
                'ocds_ftp_sync = openprocurement.search.ocds_ftp_sync:main',
                'test_search = openprocurement.search.test_search:main',
                'update_orgs = openprocurement.search.update_orgs:main',
            ],
            'paste.app_factory': [
                'search_server = openprocurement.search.search_server:make_app'
            ]
        }
    )

