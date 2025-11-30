import mysql.connector
import pytest

from config.settings import DatabaseSettings
from db import connection as connection_module
from db.connection import create_database_connection, database_connection_scope


class DummyConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_create_database_connection_calls_mysql_connector_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs = {}

    def fake_connect(**kwargs):
        captured_kwargs.update(kwargs)
        return DummyConnection()

    monkeypatch.setattr(mysql.connector, "connect", fake_connect)

    settings = DatabaseSettings(
        host="localhost",
        port=3306,
        user="user",
        password="password",
        name="database",
        charset="utf8mb4",
    )

    connection = create_database_connection(settings)

    assert isinstance(connection, DummyConnection)
    assert captured_kwargs["host"] == "localhost"
    assert captured_kwargs["port"] == 3306
    assert captured_kwargs["user"] == "user"
    assert captured_kwargs["password"] == "password"
    assert captured_kwargs["database"] == "database"
    assert captured_kwargs["charset"] == "utf8mb4"


def test_create_database_connection_propagates_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_connect(**kwargs):
        raise mysql.connector.Error("connection failed")

    monkeypatch.setattr(mysql.connector, "connect", fake_connect)

    settings = DatabaseSettings(
        host="localhost",
        port=3306,
        user="user",
        password="password",
        name="database",
        charset="utf8mb4",
    )

    with pytest.raises(mysql.connector.Error):
        create_database_connection(settings)


def test_database_connection_scope_yields_and_closes_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_connection = DummyConnection()

    def fake_create_database_connection(database_settings: DatabaseSettings):
        return dummy_connection

    monkeypatch.setattr(connection_module, "create_database_connection", fake_create_database_connection)

    settings = DatabaseSettings(
        host="localhost",
        port=3306,
        user="user",
        password="password",
        name="database",
        charset="utf8mb4",
    )

    with database_connection_scope(settings) as connection:
        assert connection is dummy_connection
        assert dummy_connection.closed is False

    assert dummy_connection.closed is True


def test_database_connection_scope_closes_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_connection = DummyConnection()

    def fake_create_database_connection(database_settings: DatabaseSettings):
        return dummy_connection

    monkeypatch.setattr(connection_module, "create_database_connection", fake_create_database_connection)

    settings = DatabaseSettings(
        host="localhost",
        port=3306,
        user="user",
        password="password",
        name="database",
        charset="utf8mb4",
    )

    with pytest.raises(RuntimeError):
        with database_connection_scope(settings):
            raise RuntimeError("failure inside context")

    assert dummy_connection.closed is True

