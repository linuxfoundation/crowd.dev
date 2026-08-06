"""
Pytest configuration and fixtures for git integration tests.

This file sets up the test environment before any test modules are imported.
"""

import os


def pytest_configure(config):
    """
    Pytest hook called before test collection.
    Sets test environment variables before any modules are imported.
    """
    test_env = {
        "CROWD_DB_WRITE_HOST": "localhost",
        "CROWD_DB_PORT": "9999",
        "CROWD_DB_USERNAME": "test_user",
        "CROWD_DB_PASSWORD": "test_pass",
        "CROWD_DB_DATABASE": "test_db",
        "CROWD_KAFKA_BROKERS": "localhost",
        "CROWD_KAFKA_TOPIC": "test-activities",
        "MAX_CONCURRENT_ONBOARDINGS": "3",
        "WORKER_POLLING_INTERVAL_SEC": "5",
        "WORKER_ERROR_BACKOFF_SEC": "10",
        "REPOSITORY_UPDATE_INTERVAL_HOURS": "24",
        "MAINTAINER_RETRY_INTERVAL_DAYS": "30",
        "MAINTAINER_UPDATE_INTERVAL_HOURS": "24",
        "AFFILIATION_RETRY_INTERVAL_DAYS": "30",
        "AFFILIATION_UPDATE_INTERVAL_HOURS": "24",
        "WORKER_SHUTDOWN_TIMEOUT_SEC": "3600",
    }

    # Set environment variables (only if not already set)
    for key, value in test_env.items():
        os.environ.setdefault(key, value)
