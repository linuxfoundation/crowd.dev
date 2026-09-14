from typing import Any

from loguru import logger

from crowdmail.errors import InternalError

from .connection import get_db_connection


async def query(sql: str, params: tuple = None) -> list[dict[str, Any]]:
    """Execute query with connection pooling"""
    try:
        async with get_db_connection() as conn:
            results = await conn.fetch(sql, *params) if params else await conn.fetch(sql)
            return [dict(row) for row in results]
    except Exception as error:
        logger.error("Database query failed - SQL: {}, Params: {}, Error: {}", sql, params, error)
        raise InternalError("Database query failed") from error


async def execute(sql: str, params: tuple = None) -> str:
    """Execute write query with connection pooling"""
    try:
        async with get_db_connection() as conn:
            result = await conn.execute(sql, *params) if params else await conn.execute(sql)
            return result
    except Exception as error:
        logger.error(
            "Database write operation failed - SQL: {}, Params: {}, Error: {}", sql, params, error
        )
        raise InternalError("Database execute operation failed") from error


async def executemany(sql: str, params_list: list[tuple]) -> str:
    """Execute write query multiple times with connection pooling"""
    try:
        async with get_db_connection() as conn:
            result = await conn.executemany(sql, params_list)
            return result
    except Exception as error:
        logger.error(
            "Database executemany operation failed - SQL: {}, Params count: {}, Error: {}",
            sql,
            len(params_list) if params_list else 0,
            error,
        )
        raise InternalError("Database executemany operation failed") from error


async def fetchval(sql: str, params: tuple = None) -> Any:
    """Execute query and return single value"""
    try:
        async with get_db_connection() as conn:
            result = await conn.fetchval(sql, *params) if params else await conn.fetchval(sql)
            return result
    except Exception as error:
        logger.error(
            "Database fetchval failed - SQL: {}, Params: {}, Error: {}", sql, params, error
        )
        raise InternalError("Database fetchval failed") from error


async def fetchrow(sql: str, params: tuple = None) -> dict[str, Any] | None:
    """Execute query and return single row"""
    try:
        async with get_db_connection() as conn:
            result = await conn.fetchrow(sql, *params) if params else await conn.fetchrow(sql)
            return dict(result) if result else None
    except Exception as error:
        logger.error(
            "Database fetchrow failed - SQL: {}, Params: {}, Error: {}", sql, params, error
        )
        raise InternalError("Database fetchrow failed") from error
