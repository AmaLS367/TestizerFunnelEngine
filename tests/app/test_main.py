from contextlib import contextmanager
from typing import Optional
from unittest.mock import MagicMock

import pytest

import sentry_sdk

from app import main as app_main


class DummyApplicationSettings:
    def __init__(self) -> None:
        self.environment = "test"
        self.dry_run = True
        self.log_level = "INFO"


class DummyDatabaseSettings:
    def __init__(self) -> None:
        self.host = "localhost"


class DummyBrevoSettings:
    def __init__(self) -> None:
        self.api_key = "test-key"
        self.base_url = "https://api.brevo.com/v3"
        self.language_tests_list_id = 4
        self.non_language_tests_list_id = 5


class DummySentrySettings:
    def __init__(self) -> None:
        self.dsn = None


class DummySettings:
    def __init__(self) -> None:
        self.application = DummyApplicationSettings()
        self.database = DummyDatabaseSettings()
        self.brevo = DummyBrevoSettings()
        self.sentry = DummySentrySettings()


class DummyConnection:
    pass


class FakeBrevoApiClient:
    def __init__(self, api_key: str, base_url: str, dry_run: bool) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.dry_run = dry_run


class FakeFunnelSyncService:
    def __init__(
        self,
        connection: DummyConnection,
        brevo_client: FakeBrevoApiClient,
        language_list_id: int,
        non_language_list_id: int,
    ) -> None:
        self.connection = connection
        self.brevo_client = brevo_client
        self.language_list_id = language_list_id
        self.non_language_list_id = non_language_list_id
        self.sync_called_with: Optional[int] = None

    def sync(self, max_rows_per_type: int) -> None:
        self.sync_called_with = max_rows_per_type


class FakePurchaseSyncService:
    def __init__(
        self, connection: DummyConnection, brevo_client: FakeBrevoApiClient, dry_run=False
    ) -> None:
        self.connection = connection
        self.brevo_client = brevo_client
        self.sync_called_with: Optional[int] = None

    def sync(self, max_rows: int) -> None:
        self.sync_called_with = max_rows


@contextmanager
def fake_database_connection_scope(database_settings):
    yield DummyConnection()


def test_main_runs_full_cycle_with_configured_lists(monkeypatch) -> None:
    settings = DummySettings()
    funnel_service_instances = []
    purchase_service_instances = []

    def fake_load_settings():
        return settings

    def fake_configure_logging(log_level: str) -> None:
        return None

    def fake_funnel_service_factory(
        connection, brevo_client, language_list_id, non_language_list_id, dry_run=False
    ):
        instance = FakeFunnelSyncService(
            connection, brevo_client, language_list_id, non_language_list_id
        )
        funnel_service_instances.append(instance)
        return instance

    def fake_purchase_service_factory(connection, brevo_client, dry_run=False):
        instance = FakePurchaseSyncService(connection, brevo_client, dry_run)
        purchase_service_instances.append(instance)
        return instance

    monkeypatch.setattr(app_main, "load_settings", fake_load_settings)
    monkeypatch.setattr(app_main, "configure_logging", fake_configure_logging)
    monkeypatch.setattr(
        app_main, "database_connection_scope", fake_database_connection_scope
    )
    monkeypatch.setattr(app_main, "BrevoApiClient", FakeBrevoApiClient)
    monkeypatch.setattr(app_main, "FunnelSyncService", fake_funnel_service_factory)
    monkeypatch.setattr(app_main, "PurchaseSyncService", fake_purchase_service_factory)

    app_main.main()

    assert len(funnel_service_instances) == 1
    assert len(purchase_service_instances) == 1

    funnel_service = funnel_service_instances[0]
    purchase_service = purchase_service_instances[0]

    assert funnel_service.language_list_id == 4
    assert funnel_service.non_language_list_id == 5
    assert funnel_service.sync_called_with == 10
    assert purchase_service.sync_called_with == 100


def test_main_exits_early_when_lists_not_configured(monkeypatch) -> None:
    settings = DummySettings()
    settings.brevo.language_tests_list_id = 0
    settings.brevo.non_language_tests_list_id = 0

    funnel_service_instances = []
    purchase_service_instances = []

    def fake_load_settings():
        return settings

    def fake_configure_logging(log_level: str) -> None:
        return None

    monkeypatch.setattr(app_main, "load_settings", fake_load_settings)
    monkeypatch.setattr(app_main, "configure_logging", fake_configure_logging)
    monkeypatch.setattr(
        app_main, "database_connection_scope", fake_database_connection_scope
    )
    monkeypatch.setattr(app_main, "BrevoApiClient", FakeBrevoApiClient)
    monkeypatch.setattr(
        app_main,
        "FunnelSyncService",
        lambda *args, **kwargs: funnel_service_instances.append(args),
    )
    monkeypatch.setattr(
        app_main,
        "PurchaseSyncService",
        lambda *args, **kwargs: purchase_service_instances.append(args),
    )

    app_main.main()

    assert funnel_service_instances == []
    assert purchase_service_instances == []


def test_main_handles_exception_and_exits_with_code_1(monkeypatch) -> None:
    sentry_calls = []
    logger_calls = []

    mock_logger = MagicMock()
    mock_logger.critical = lambda *args, **kwargs: logger_calls.append((args, kwargs))

    settings = DummySettings()

    def fake_load_settings():
        return settings

    def fake_configure_logging(log_level: str) -> None:
        return None

    import logging

    original_get_logger = logging.getLogger

    def fake_get_logger(name=None):
        if name == "app.main":
            return mock_logger
        return original_get_logger(name)

    def fake_capture_exception(exception):
        sentry_calls.append(exception)

    def fake_database_connection_scope(database_settings):
        raise RuntimeError("Database connection failed")

    monkeypatch.setattr(app_main, "load_settings", fake_load_settings)
    monkeypatch.setattr(app_main, "configure_logging", fake_configure_logging)
    monkeypatch.setattr(logging, "getLogger", fake_get_logger)
    monkeypatch.setattr(
        app_main, "database_connection_scope", fake_database_connection_scope
    )
    monkeypatch.setattr(sentry_sdk, "capture_exception", fake_capture_exception)

    with pytest.raises(SystemExit) as exc_info:
        app_main.main()

    assert exc_info.value.code == 1
    assert len(sentry_calls) == 1
    assert isinstance(sentry_calls[0], RuntimeError)
    assert str(sentry_calls[0]) == "Database connection failed"
    assert len(logger_calls) == 1
    assert "Critical error in main" in logger_calls[0][0][0]


def test_main_propagates_keyboard_interrupt(monkeypatch) -> None:
    def fake_load_settings():
        raise KeyboardInterrupt()

    def fake_configure_logging(log_level: str) -> None:
        logger = MagicMock()
        logger.info = MagicMock()
        import logging

        def fake_get_logger(name: Optional[str] = None):  # type: ignore[assignment]
            return logger

        logging.getLogger = fake_get_logger
        return None

    monkeypatch.setattr(app_main, "load_settings", fake_load_settings)
    monkeypatch.setattr(app_main, "configure_logging", fake_configure_logging)

    with pytest.raises(KeyboardInterrupt):
        app_main.main()
