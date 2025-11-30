from config import settings as settings_module


def test_load_boolean_uses_default_when_env_missing(monkeypatch):
    monkeypatch.delenv("APP_DRY_RUN", raising=False)
    result = settings_module._load_boolean("APP_DRY_RUN", True)
    assert result is True


def test_load_boolean_parses_truthy_and_falsy_values(monkeypatch):
    monkeypatch.setenv("TEST_BOOL", "true")
    result_true = settings_module._load_boolean("TEST_BOOL", False)
    assert result_true is True

    monkeypatch.setenv("TEST_BOOL", "0")
    result_false = settings_module._load_boolean("TEST_BOOL", True)
    assert result_false is False


def test_load_integer_uses_default_for_missing_or_invalid(monkeypatch):
    monkeypatch.delenv("TEST_INT", raising=False)
    result_missing = settings_module._load_integer("TEST_INT", 5)
    assert result_missing == 5

    monkeypatch.setenv("TEST_INT", "not_an_integer")
    result_invalid = settings_module._load_integer("TEST_INT", 5)
    assert result_invalid == 5


def test_load_settings_builds_configuration_from_env(monkeypatch):
    def fake_load_dotenv():
        return None

    monkeypatch.setattr(settings_module, "load_dotenv", fake_load_dotenv)

    monkeypatch.setenv("DB_HOST", "db-host")
    monkeypatch.setenv("DB_PORT", "3307")
    monkeypatch.setenv("DB_USER", "db-user")
    monkeypatch.setenv("DB_PASSWORD", "db-password")
    monkeypatch.setenv("DB_NAME", "db-name")
    monkeypatch.setenv("DB_CHARSET", "utf8mb4")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_DRY_RUN", "0")
    monkeypatch.setenv("APP_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("BREVO_API_KEY", "brevo-key")
    monkeypatch.setenv("BREVO_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("BREVO_LANGUAGE_LIST_ID", "10")
    monkeypatch.setenv("BREVO_NON_LANGUAGE_LIST_ID", "20")
    monkeypatch.setenv("SENTRY_DSN", "https://example@sentry.io/123456")

    config = settings_module.load_settings()

    assert config.database.host == "db-host"
    assert config.database.port == 3307
    assert config.database.user == "db-user"
    assert config.database.password == "db-password"
    assert config.database.name == "db-name"
    assert config.database.charset == "utf8mb4"
    assert config.application.environment == "production"
    assert config.application.dry_run is False
    assert config.application.log_level == "DEBUG"
    assert config.brevo.api_key == "brevo-key"
    assert config.brevo.base_url == "https://api.example.com"
    assert config.brevo.language_tests_list_id == 10
    assert config.brevo.non_language_tests_list_id == 20
    assert config.sentry.dsn == "https://example@sentry.io/123456"

