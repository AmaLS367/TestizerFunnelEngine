import logging

from config.settings import load_settings
from db.connection import database_connection_scope
from db.selectors import (
    get_language_test_candidates,
    get_non_language_test_candidates,
)
from logging_config.logger import configure_logging


def main() -> None:
    """
    Main entry point for the periodic job.

    On Stage 1 this function:
    - loads settings,
    - configures logging,
    - connects to the database,
    - executes placeholder selectors,
    - logs results.
    """
    settings = load_settings()
    configure_logging(settings.application.log_level)

    logger = logging.getLogger("app.main")

    logger.info("Application environment: %s", settings.application.environment)
    logger.info("Dry run mode: %s", settings.application.dry_run)

    logger.info("Connecting to database")
    with database_connection_scope(settings.database) as connection:
        logger.info("Connected to database successfully")

        logger.info("Fetching language test candidates (placeholder)")
        language_candidates = get_language_test_candidates(connection, max_rows=10)
        logger.info("Language candidates count: %s", len(language_candidates))

        logger.info("Fetching non language test candidates (placeholder)")
        non_language_candidates = get_non_language_test_candidates(connection, max_rows=10)
        logger.info("Non language candidates count: %s", len(non_language_candidates))

        if language_candidates:
            logger.info("First language candidate row: %s", language_candidates[0])

        if non_language_candidates:
            logger.info("First non language candidate row: %s", non_language_candidates[0])

    logger.info("Job finished")


if __name__ == "__main__":
    main()
