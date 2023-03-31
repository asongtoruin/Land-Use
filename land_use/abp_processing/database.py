# -*- coding: utf-8 -*-
"""Functionality for connecting to the PostgreSQL Address Based Premium database."""

##### IMPORTS #####
# Standard imports
from __future__ import annotations
import logging
from typing import Any, Mapping, Sequence, TypeAlias

# Third party imports
import pandas as pd
import psycopg2
from psycopg2 import sql
from pydantic import dataclasses

# Local imports

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####
_Query: TypeAlias = str | sql.SQL
_Vars: TypeAlias = Sequence[Any] | Mapping[str, Any] | None


@dataclasses.dataclass  # pylint: disable=c-extension-no-member
class ConnectionParameters:
    """Parameters for connecting to the PostgreSQL database."""

    database: str
    user: str
    password: str
    host: str
    port: int
    application_name: str | None = None


class Database:
    """Manage connection and access to a PostgreSQL database."""

    def __init__(self, parameters: ConnectionParameters) -> None:
        self._parameters = parameters
        self._connection = self._connect(parameters)
        self._cursor = self._connection.cursor()

    def _connect(self, parameters: ConnectionParameters) -> psycopg2.connection:
        app_name = parameters.application_name
        if app_name is None:
            app_name = __name__

        connection = psycopg2.connect(
            database=parameters.database,
            user=parameters.user,
            password=parameters.password,
            host=parameters.host,
            port=parameters.port,
            options="-c search_path=dbo,data_common",
            application_name=app_name,
        )
        LOG.info("Connected to database: %s", parameters.database)

        return connection

    def __enter__(self) -> Database:
        """Initialise Database."""
        return self

    def __exit__(self, excepType, excepVal, traceback) -> None:
        """Close database connection."""
        if excepType is not None or excepVal is not None or traceback is not None:
            LOG.critical("Oh no a critical error occurred", exc_info=True)

        self._cursor.close()
        self._connection.close()
        LOG.info("Database connection closed")

    @property
    def connection(self) -> psycopg2.connection:
        """PostgreSQL database connection."""
        return self._connection

    def execute(self, query: _Query, vars_: _Vars = None) -> None:
        """Execute `query` on database."""
        LOG.debug(
            "Executing query on %s:\n%s",
            self._parameters.database,
            self._cursor.mogrify(query, vars=vars_).decode(),
        )
        self._cursor.execute(query, vars_)

    def query_fetch(self, query: _Query, vars_: _Vars = None) -> list[tuple[Any]]:
        """Query database and return all rows found."""
        self.execute(query, vars_)
        return self._cursor.fetchall()

    def query_to_dataframe(
        self, query: _Query, query_vars: _Vars = None
    ) -> pd.DataFrame:
        """Query database and convert rows to DataFrame."""
        return pd.DataFrame(
            self.query_fetch(query, query_vars),
            columns=[desc[0] for desc in self._cursor.description],
        )


##### FUNCTIONS #####
