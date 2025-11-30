import logging
import sys
import traceback
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from config.settings import load_settings
from db.connection import database_connection_scope
from logging_config.logger import configure_logging
from brevo.api_client import BrevoApiClient
from funnels.sync_service import FunnelSyncService
from funnels.purchase_sync_service import PurchaseSyncService


def _init_sentry(dsn: Optional[str], environment: str) -> None:
    if not dsn:
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        integrations=[
            LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR,
            ),
        ],
        traces_sample_rate=1.0,
    )


def _setup_global_exception_handler(logger: logging.Logger) -> None:
    def exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        error_message = "".join(
            traceback.format_exception(exc_type, exc_value, exc_traceback)
        )
        logger.critical(
            "Unhandled exception: %s",
            error_message,
            exc_info=(exc_type, exc_value, exc_traceback),
        )

        sentry_sdk.capture_exception(exc_value)

    sys.excepthook = exception_handler


def main() -> None:
    logger: Optional[logging.Logger] = None

    try:
        settings = load_settings()
        configure_logging(settings.application.log_level)

        logger = logging.getLogger("app.main")

        _init_sentry(settings.sentry.dsn, settings.application.environment)
        _setup_global_exception_handler(logger)

        logger.info("Application environment: %s", settings.application.environment)
        logger.info("Dry run mode: %s", settings.application.dry_run)

        logger.info("Connecting to database")
        with database_connection_scope(settings.database) as connection:
            logger.info("Connected to database successfully")

            if (
                settings.brevo.language_tests_list_id <= 0
                and settings.brevo.non_language_tests_list_id <= 0
            ):
                logger.info(
                    "Brevo list ids are not configured, skipping funnel synchronization",
                )
                return

            brevo_client = BrevoApiClient(
                api_key=settings.brevo.api_key or "",
                base_url=settings.brevo.base_url,
                dry_run=settings.application.dry_run,
            )

            funnel_sync_service = FunnelSyncService(
                connection=connection,
                brevo_client=brevo_client,
                language_list_id=settings.brevo.language_tests_list_id,
                non_language_list_id=settings.brevo.non_language_tests_list_id,
            )

            funnel_sync_service.sync(max_rows_per_type=10)

            purchase_sync_service = PurchaseSyncService(
                connection=connection,
                brevo_client=brevo_client,
            )

            purchase_sync_service.sync(max_rows=100)

        logger.info("Job finished")

    except KeyboardInterrupt:
        if logger:
            logger.info("Job interrupted by user")
        raise

    except Exception as e:
        if logger:
            logger.critical(
                "Critical error in main: %s",
                str(e),
                exc_info=True,
            )
        sentry_sdk.capture_exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
