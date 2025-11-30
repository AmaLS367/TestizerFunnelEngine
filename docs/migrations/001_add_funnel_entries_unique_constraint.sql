-- This migration enforces idempotency at the database level for funnel entries.
-- It adds a unique constraint on (email, funnel_type, test_id) to prevent duplicate entries.
-- Any attempt to insert a duplicate entry for the same triple will fail with a unique constraint error.

ALTER TABLE funnel_entries ADD UNIQUE KEY uk_funnel_entry_email_type_test (email, funnel_type, test_id);

