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

