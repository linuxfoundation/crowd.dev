"""
Loguru logger configuration
Replaces the old standard logging configuration with loguru for services
"""

import sys

from loguru import logger


def format_record(record):
    """Custom format function that only shows extra fields when they exist"""
    format_str = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line}"

    if record["extra"]:
        extra_parts = []
        for key, value in record["extra"].items():
            extra_parts.append(f"{key}={value}")
        if extra_parts:
            format_str += f" | [{', '.join(extra_parts)}]"

    format_str += " - {message}\n"
    return format_str


logger.remove()  # Remove default handler
logger.add(sys.stderr, format=format_record, level="INFO", colorize=True)

__all__ = ["logger"]
