#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-mssqlserver"
package_version = "0.0.1"
description = """The Microsoft SQL Server adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Bradley Nielsen",
    author_email="brad.e.nielsen@gmail.com",
    url="https://github.com/nielsenbe/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/mssqlserver/dbt_project.yml',
            'include/rmssqlserver/macros/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'dbt-postgres=={}'.format(package_version),
        'boto3>=1.6.23,<1.10.0',
        'botocore>=1.9.23,<1.13.0',
        'psycopg2>=2.7.5,<2.8',
    ]
)
