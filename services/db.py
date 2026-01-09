# services/db.py (Consolidated PG helper)
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import threading
import logging

# Import config safely
try:
    from config import DATABASE_URL, DB_POOL_SIZE
except ImportError:
    import os
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/vigilbuild")
    DB_POOL_SIZE = 20

logger = logging.getLogger(__name__)

# Thread-safe pool initialization
_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    """Get or create the connection pool (thread-safe singleton)"""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:  # Double-check after acquiring lock
                try:
                    _pool = SimpleConnectionPool(1, DB_POOL_SIZE, dsn=DATABASE_URL)
                    logger.info(f"Database connection pool initialized with max {DB_POOL_SIZE} connections")
                except Exception as e:
                    logger.error(f"Failed to initialize database pool: {e}")
                    raise
    return _pool


def get_connection():
    """
    Get a connection from the pool.

    IMPORTANT: Always use db_connection() context manager instead when possible.
    If you must use get_connection() directly, you MUST call release_connection()
    in a finally block to prevent connection leaks.

    Preferred usage:
        with db_connection() as conn:
            cursor = conn.cursor()
            # ... do work ...
    """
    pool = _get_pool()
    try:
        conn = pool.getconn()
        if conn is None:
            raise Exception("Could not get connection from pool - pool may be exhausted")
        return conn
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        raise


def release_connection(conn):
    """Release a connection back to the pool"""
    if conn is None:
        return
    pool = _get_pool()
    try:
        pool.putconn(conn)
    except Exception as e:
        logger.error(f"Failed to release connection: {e}")


@contextmanager
def db_connection():
    """
    Context manager for safe database connection handling.
    Automatically releases connection back to pool when done.

    Usage:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users")
            results = cursor.fetchall()
            conn.commit()  # if needed
    """
    conn = None
    try:
        conn = get_connection()
        yield conn
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            release_connection(conn)


@contextmanager
def db_transaction():
    """
    Context manager for database transactions with automatic commit/rollback.

    Usage:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO ...")
            cursor.execute("UPDATE ...")
            # Automatically commits on success, rolls back on exception
    """
    conn = None
    try:
        conn = get_connection()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error(f"Transaction error: {e}")
        raise
    finally:
        if conn:
            release_connection(conn)


def close_pool():
    """Close all connections in the pool (call on application shutdown)"""
    global _pool
    if _pool:
        try:
            _pool.closeall()
            logger.info("Database connection pool closed")
        except Exception as e:
            logger.error(f"Error closing pool: {e}")
        _pool = None


def get_pool_status():
    """Get current pool status for monitoring"""
    pool = _get_pool()
    if pool:
        # SimpleConnectionPool doesn't expose these directly, but we can track usage
        return {
            "min_connections": pool.minconn,
            "max_connections": pool.maxconn,
        }
    return None