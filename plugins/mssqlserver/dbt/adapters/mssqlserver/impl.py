from dbt.adapters.sql import SQLAdapter
from dbt.adapters.MsSqlServer import MsSqlServerConnectionManager


class MsSqlServerAdapter(SQLAdapter):
    ConnectionManager = MsSqlServerConnectionManager
