from dbt.adapters.MsSqlServer.connections import MsSqlServerConnectionManager
from dbt.adapters.MsSqlServer.connections import MsSqlServerCredentials
from dbt.adapters.MsSqlServer.impl import MsSqlServerAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import MsSqlServer


Plugin = AdapterPlugin(
    adapter=MsSqlServerAdapter,
    credentials=MsSqlServerCredentials,
    include_path=MsSqlServer.PACKAGE_PATH)
