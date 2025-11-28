import logging

from config.settings import load_settings
from db.connection import database_connection_scope
from logging_config.logger import configure_logging
from brevo.api_client import BrevoApiClient
from funnels.sync_service import FunnelSyncService


def main() -> None:
    settings = load_settings()
    configure_logging(settings.application.log_level)

    logger = logging.getLogger("app.main")

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

    logger.info("Job finished")


if __name__ == "__main__":
    main()
