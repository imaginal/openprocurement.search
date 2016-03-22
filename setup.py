from setuptools import setup, find_packages


version = '0.1'


setup(name='openprocurement.search',
        version=version,
        description="OpenProcurement search service and indexer",
        long_description=open("README.md").read(),
        # Get more strings from
        # http://pypi.python.org/pypi?:action=list_classifiers
        classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        ],
        keywords='OpenProcurement',
        author='E-Democracy NGO',
        author_email='info@ed.org.ua',
        license='Apache License 2.0',
        url='https://github.com/imaginal/openprocurement.search',
        packages=find_packages(),
        namespace_packages=['openprocurement'],
        include_package_data=True,
        zip_safe=False,
        install_requires=[
          'setuptools',
          'requests',
          'iso8601',
          'python-dateutil',
          'simplejson',
          'Flask',
          'gevent',
          'PyYAML',
          # 'sse',
          # 'request_id_middleware',
          # ssl warning
          'openprocurement_client',
          'elasticsearch==1.9.0',
        ],
        entry_points={
          'console_scripts': [
              'index_worker = openprocurement.search.index_worker:main',
              'ftpsync = openprocurement.search.ftpsync:main',
           ],
          'paste.app_factory': [
              'search_server = openprocurement.search.search_server:make_app'
          ]
        }
        )
