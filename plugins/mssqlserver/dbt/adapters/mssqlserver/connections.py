from contextlib import contextmanager

import pyodbc

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger

MSSQLSERVER_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
    },
    'required': ['database', 'schema'],
}


class MsSqlServerCredentials(Credentials):
    SCHEMA = MSSQLSERVER_CREDENTIALS_CONTRACT

    @property
    def type(self):
        return 'mssqlserver'

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ('host', 'port', 'user', 'database', 'schema')


class MsSqlServerConnectionManager(SQLConnectionManager):
    TYPE = 'mssqlserver'


    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        base_credentials = connection.credentials

        try:
            handle =