import logging
import logging.handlers
from pathlib import Path

from logging_config.logger import configure_logging


def test_configure_logging_creates_directory_and_handlers(tmp_path) -> None:
    logs_dir = tmp_path / "logs_dir"

    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    configure_logging("INFO", logs_directory=str(logs_dir))

    assert logs_dir.exists()
    assert logs_dir.is_dir()
    assert len(root_logger.handlers) == 2

    levels = {handler.level for handler in root_logger.handlers if handler.level != 0}
    assert logging.INFO in levels or root_logger.level == logging.INFO

    file_handlers = [
        handler
        for handler in root_logger.handlers
        if isinstance(handler, logging.handlers.RotatingFileHandler)
    ]

    assert len(file_handlers) == 1
    file_handler = file_handlers[0]
    assert Path(file_handler.baseFilename).name == "app.log"
