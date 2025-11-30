import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class DatabaseSettings:
    host: str
    port: int
    user: str
    password: str
    name: str
    charset: str


@dataclass
class ApplicationSettings:
    environment: str
    dry_run: bool
    log_level: str


@dataclass
class BrevoSettings:
    api_key: Optional[str]
    base_url: str
    language_tests_list_id: int
    non_language_tests_list_id: int


@dataclass
class SentrySettings:
    dsn: Optional[str]


@dataclass
class Settings:
    database: DatabaseSettings
    application: ApplicationSettings
    brevo: BrevoSettings
    sentry: SentrySettings


def _load_boolean(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    value_lower = value.strip().lower()
    return value_lower in ["1", "true", "yes", "y"]


def _load_integer(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default

    try:
        return int(value)
    except ValueError:
        return default


def load_settings() -> Settings:
    load_dotenv()

    database_settings = DatabaseSettings(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=_load_integer("DB_PORT", 3306),
        user=os.getenv("DB_USER", "testizer_user"),
        password=os.getenv("DB_PASSWORD", "change_me"),
        name=os.getenv("DB_NAME", "testizer"),
        charset=os.getenv("DB_CHARSET", "utf8mb4"),
    )

    application_settings = ApplicationSettings(
        environment=os.getenv("APP_ENV", "development"),
        dry_run=_load_boolean("APP_DRY_RUN", True),
        log_level=os.getenv("APP_LOG_LEVEL", "INFO"),
    )

    brevo_settings = BrevoSettings(
        api_key=os.getenv("BREVO_API_KEY"),
        base_url=os.getenv("BREVO_BASE_URL", "https://api.brevo.com/v3"),
        language_tests_list_id=_load_integer("BREVO_LANGUAGE_LIST_ID", 0),
        non_language_tests_list_id=_load_integer("BREVO_NON_LANGUAGE_LIST_ID", 0),
    )

    sentry_settings = SentrySettings(
        dsn=os.getenv("SENTRY_DSN"),
    )

    return Settings(
        database=database_settings,
        application=application_settings,
        brevo=brevo_settings,
        sentry=sentry_settings,
    )
