"""Repository functions for brevo_sync_outbox table.

This module provides functions to enqueue, fetch, and update Brevo synchronization
jobs stored in the brevo_sync_outbox table. All functions are transaction-friendly
and do not perform commits or rollbacks - transaction control is the caller's responsibility.
"""

import json
from dataclasses import dataclass
from typing import List

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
            retry_count
        FROM brevo_sync_outbox
        WHERE status = 'pending'
        ORDER BY id
        LIMIT %s
        """

        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        jobs = []
        for row in rows:
            job = BrevoSyncJob(
                id=row[0],
                funnel_entry_id=row[1],
                operation_type=row[2],
                payload=row[3],
                status=row[4],
                retry_count=row[5],
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
) -> None:
    """Marks a job as failed with an error message.

    Updates the job row to set status='error', last_error=error_message,
    and increments retry_count by 1.

    Args:
        connection: Active MySQL database connection.
        job_id: ID of the job to mark as failed.
        error_message: Error message describing the failure.

    Raises:
        mysql.connector.Error: If database update fails.
    """
    cursor = connection.cursor()

    try:
        query = """
        UPDATE brevo_sync_outbox
        SET status = 'error',
            last_error = %s,
            retry_count = retry_count + 1
        WHERE id = %s
        """

        cursor.execute(query, (error_message, job_id))

    finally:
        cursor.close()

