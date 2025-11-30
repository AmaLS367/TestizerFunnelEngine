-- Test schema for integration tests
-- This schema must be kept in sync with production schema for overlapping tables
-- (funnel_entries and brevo_sync_outbox) to ensure test accuracy.

-- Simple test tables for MODX database simulation
CREATE TABLE IF NOT EXISTS simpletest_users (
  Id INT NOT NULL AUTO_INCREMENT,
  Email VARCHAR(255) NULL,
  TestId INT NULL,
  Datep DATETIME NULL,
  Status INT NULL,
  PRIMARY KEY (Id),
  KEY idx_testid (TestId),
  KEY idx_datep (Datep)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS simpletest_test (
  Id INT NOT NULL AUTO_INCREMENT,
  LangId INT NULL,
  PRIMARY KEY (Id),
  KEY idx_langid (LangId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS simpletest_lang (
  Id INT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (Id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Analytics tables (must match production schema)
CREATE TABLE IF NOT EXISTS funnel_entries (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  funnel_type VARCHAR(50) NOT NULL,
  user_id INT NULL,
  test_id INT NULL,
  entered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  certificate_purchased TINYINT(1) NOT NULL DEFAULT 0,
  certificate_purchased_at DATETIME NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_funnel_entry_email_type_test (email, funnel_type, test_id),
  KEY idx_email_funnel (email, funnel_type),
  KEY idx_user_test (user_id, test_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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

