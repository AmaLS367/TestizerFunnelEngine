"""Repository functions for brevo_sync_outbox table.

This module provides functions to enqueue, fetch, and update Brevo synchronization
jobs stored in the brevo_sync_outbox table. All functions are transaction-friendly
and do not perform commits or rollbacks - transaction control is the caller's responsibility.
"""

from dataclasses import dataclass
from typing import List, Optional

import mysql.connector
from mysql.connector import MySQLConnection


@dataclass
class BrevoSyncJob:
    """Represents a Brevo synchronization job from the outbox table."""

    id: int
    funnel_entry_id: int
    operation_type: str
    payload: str
    status: str
    retry_count: int
    next_attempt_at: Optional[str] = None  # DATETIME as string or None


def enqueue_brevo_sync_job(
    connection: MySQLConnection,
    funnel_entry_id: int,
    operation_type: str,
    payload: str,
) -> int:
    """Enqueues a new Brevo synchronization job in the outbox.

    Inserts a new row into brevo_sync_outbox with status='pending' and retry_count=0.

    Args:
        connection: Active MySQL database connection.
        funnel_entry_id: ID of the associated funnel entry.
        operation_type: Type of operation (e.g., 'upsert_contact', 'update_after_purchase').
        payload: JSON string containing operation-specific data.

    Returns:
        The ID of the newly created job row.

    Raises:
        mysql.connector.Error: If database insert fails.
    """
    cursor = connection.cursor()

    try:
        query = """
        INSERT INTO brevo_sync_outbox (
            funnel_entry_id,
            operation_type,
            payload,
            status,
            retry_count
        ) VALUES (%s, %s, %s, 'pending', 0)
        """

        params = (funnel_entry_id, operation_type, payload)
        cursor.execute(query, params)
        new_id = cursor.lastrowid
        if new_id is None:
            raise mysql.connector.Error("Failed to get inserted row ID")
        return new_id

    finally:
        cursor.close()


def fetch_pending_jobs(
    connection: MySQLConnection,
    limit: int = 100,
) -> List[BrevoSyncJob]:
    """Fetches pending jobs from the outbox.

    Selects rows where status='pending', ordered by id, limited by limit.

    Args:
        connection: Active MySQL database connection.
        limit: Maximum number of jobs to fetch. Defaults to 100.

    Returns:
        List of BrevoSyncJob instances representing pending jobs.

    Raises:
        mysql.connector.Error: If database query fails.
    """
    cursor = connection.cursor()

    try:
        query = """
        SELECT
            id,
            funnel_entry_id,
            operation_type,
            payload,
            status,
            retry_count,
            next_attempt_at
        FROM brevo_sync_outbox
        WHERE status = 'pending'
          AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
        ORDER BY id
        LIMIT %s
        """

        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        jobs = []
        for row in rows:
            job = BrevoSyncJob(
                id=int(row[0]),  # type: ignore[arg-type]
                funnel_entry_id=int(row[1]),  # type: ignore[arg-type]
                operation_type=str(row[2]),  # type: ignore[arg-type]
                payload=str(row[3]),  # type: ignore[arg-type]
                status=str(row[4]),  # type: ignore[arg-type]
                retry_count=int(row[5]),  # type: ignore[arg-type]
                next_attempt_at=str(row[6]) if row[6] is not None else None,  # type: ignore[arg-type]
            )
            jobs.append(job)

        return jobs

    finally:
        cursor.close()


def mark_job_success(connection: MySQLConnection, job_id: int) -> None:
    """Marks a job as successfully completed.

    Updates the job row to set status='success' and last_error=NULL.

    Args:
        connection: Active MySQL database connection.
        job_id: ID of the job to mark as successful.

    Raises:
        mysql.connector.Error: If database update fails.
    """
    cursor = connection.cursor()

    try:
        query = """
        UPDATE brevo_sync_outbox
        SET status = 'success',
            last_error = NULL
        WHERE id = %s
        """

        cursor.execute(query, (job_id,))

    finally:
        cursor.close()


def mark_job_error(
    connection: MySQLConnection,
    job_id: int,
    error_message: str,
    max_job_retries: int = 5,
    is_fatal: bool = False,
) -> None:
    """Marks a job as failed with an error message and schedules retry if applicable.

    Increments retry_count by 1. If retry_count is still <= max_job_retries and
    the error is not fatal, sets status='pending' and schedules next_attempt_at.
    Otherwise, sets status='failed' and clears next_attempt_at.

    Args:
        connection: Active MySQL database connection.
        job_id: ID of the job to mark as failed.
        error_message: Error message describing the failure.
        max_job_retries: Maximum number of retry attempts. Defaults to 5.
        is_fatal: If True, marks the job as permanently failed regardless of retry_count.
            Defaults to False.

    Raises:
        mysql.connector.Error: If database update fails.
    """
    cursor = connection.cursor()

    try:
        if is_fatal:
            # Fatal errors are marked as failed immediately
            query = """
            UPDATE brevo_sync_outbox
            SET status = 'failed',
                last_error = %s,
                retry_count = retry_count + 1,
                next_attempt_at = NULL
            WHERE id = %s
            """
            cursor.execute(query, (error_message, job_id))
        else:
            # Check current retry_count to decide if we should retry
            # We need to increment and check in the same query
            query = """
            UPDATE brevo_sync_outbox
            SET retry_count = retry_count + 1,
                last_error = %s,
                status = CASE
                    WHEN retry_count + 1 <= %s THEN 'pending'
                    ELSE 'failed'
                END,
                next_attempt_at = CASE
                    WHEN retry_count + 1 <= %s THEN NOW() + INTERVAL (retry_count + 1) * 5 MINUTE
                    ELSE NULL
                END
            WHERE id = %s
            """
            cursor.execute(
                query, (error_message, max_job_retries, max_job_retries, job_id)
            )

    finally:
        cursor.close()
