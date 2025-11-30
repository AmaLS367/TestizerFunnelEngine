-- This migration creates the brevo_sync_outbox table used as an outbox pattern
-- for Brevo synchronization. This table decouples database writes from external
-- API calls, allowing reliable processing of Brevo operations with retry logic
-- and status tracking.

CREATE TABLE IF NOT EXISTS brevo_sync_outbox (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  funnel_entry_id INT UNSIGNED NOT NULL,
  operation_type VARCHAR(50) NOT NULL,
  payload TEXT NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  retry_count INT UNSIGNED NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  next_attempt_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_status_next_attempt (status, next_attempt_at),
  KEY idx_funnel_entry (funnel_entry_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

