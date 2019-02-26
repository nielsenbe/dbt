from contextlib import contextmanager

import pyodbc

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger

MSSQLSERVER_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'driver': {
            'type': 'string',
        },
        'server': {
            'type': 'string',
        },
        'database': {
            'type': 'string'
        },
        'trusted_connection': {
            'type': 'string'
        },
        'dsn': {
            'type': 'string'
        },
        'username': {
            'type': 'string',
        },
        'password': {
            'type': 'string'
        },
        'encrypt': {
            'type': 'string'
        },
        'trust_server_certificate': {
            'type': 'string'
        },
        'connection_timeout': {
            'type': 'integer'
        },
        'other_parameters': {
            'type': 'string'
        },
        'required': ['driver'],
    }
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

        credentials = connection.credentials

        try:
            # Build connection string
            cs = ""
            cs += buildConnectionString("Driver", connection.driver)
            cs += buildConnectionString("Server", connection.server)
            cs += buildConnectionString("Database", connection.database)
            cs += buildConnectionString("Trusted_Connection", connection.trusted_connection)
            cs += buildConnectionString("Dsn", connection.dsn)
            cs += buildConnectionString("Uid", connection.username)
            cs += buildConnectionString("Pwd", connection.password)
            cs += buildConnectionString("Encrypt", connection.encrypt)
            cs += buildConnectionString("TrustServerCertificate", connection.trust_server_certificate)
            cs += buildConnectionString("Connection Timeout", connection.connection_timeout)
            cs += connection.other_parameters if connection.other_parameters else ""

            handle = pyodbc.connect(cs)

            connection.handle = handle
            connection.state = 'open'
        except pyodbc.Error as e:
            logger.debug("Got an error when attempting to open a snowflake "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    @staticmethod
    def buildConnectionString(cls, key, value):
        if value != "" and value != None:
            f"{key}={value};"
        else:
            ""
             
    def cancel(self, connection):
        connection_name = connection.name
        handle = connection.handle

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        handle.rollback()
        handle.close()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))