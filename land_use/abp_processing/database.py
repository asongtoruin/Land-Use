# -*- coding: utf-8 -*-
"""Functionality for connecting to the PostgreSQL Address Based Premium database."""

##### IMPORTS #####
# Standard imports
from __future__ import annotations
import logging

# Third party imports
import psycopg2
from pydantic import dataclasses

# Local imports

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####
@dataclasses.dataclass
class ConnectionParameters:
    database: str
    user: str
    password: str
    host: str
    port: int


class ABPDatabase:

    def __init__(self, parameters: ConnectionParameters) -> None:
        self._connection = self._connect(parameters)
        self._cursor = self._connection.cursor()

    def _connect(self, parameters: ConnectionParameters) -> psycopg2.connection:
        connection = psycopg2.connect(
            database=parameters.database,
            user=parameters.user,
            password=parameters.password,
            host=parameters.host,
            port=parameters.port,
            options="-c search_path=dbo,data_common"
        )
        LOG.info("Connected to database: %s", parameters.database)

        return connection

    def __enter__(self) -> ABPDatabase:
        """Initialise ABPDatabase."""
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
        return self._connection

    @property
    def cursor(self) -> psycopg2.cursor:
        return self._cursor


##### FUNCTIONS #####
