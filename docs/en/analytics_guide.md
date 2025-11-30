# Testizer email funnels · Analytics guide

This document describes what metrics are available in the current version of the script and how to use them.

## 1. funnel_entries Table Structure

The `funnel_entries` table is used for basic funnel analytics.

### 1.1. Idempotency Guarantee

The table has a unique constraint on `(email, funnel_type, test_id)` that enforces idempotency at the database level. This constraint ensures that:

* The same user cannot be added to the same funnel multiple times for the same test.
* Duplicate records are prevented even under concurrent execution of the synchronization service.
* The service can be safely run multiple times without creating duplicate entries.

When the service attempts to insert a duplicate entry, the database constraint violation is handled gracefully: the transaction is rolled back, and an informational log message is recorded. No exception is raised, allowing the service to continue processing other candidates.

This idempotency guarantee eliminates race conditions (TOCTOU - Time-Of-Check-Time-Of-Use) that could occur when checking for existing entries before insertion. The database-level constraint provides a reliable, atomic mechanism to prevent duplicates.

Important fields:

- `email`

  User email address.

- `funnel_type`

  Funnel type:

  - `language` for language tests;

  - `non_language` for non-language tests.

- `user_id`, `test_id`

  Link to test and user in MODX database.

- `entered_at`

  Date and time when the user entered the funnel.

- `certificate_purchased`

  Flag:

  - `0` certificate not purchased;

  - `1` certificate purchased.

- `certificate_purchased_at`

  Date and time of certificate purchase. Populated during synchronization with MODX tables.

### 1.2. Checking for duplicate funnel entries

Before applying the unique constraint migration in production, operators should check for existing duplicate entries in the `funnel_entries` table. This can be done using the diagnostic script:

```powershell
python -m scripts.find_funnel_duplicates
```

The script queries the database for groups of entries that share the same `(email, funnel_type, test_id)` combination and displays them in a readable table format. The output includes:

* The duplicate email, funnel type, and test ID combination
* The count of duplicate entries for each combination
* The minimum and maximum entry IDs
* The earliest and latest entry timestamps

**Important:** If duplicates are found, they must be cleaned manually or via MySQL scripts before running the migration that adds the unique constraint. The migration will fail if duplicate entries exist when the constraint is applied.

The script is strictly read-only and does not modify the database in any way. It is safe to run on production databases for diagnostic purposes.

## 2. brevo_sync_outbox Table Structure

The `brevo_sync_outbox` table stores pending and processed jobs for Brevo synchronization. This table implements an outbox pattern, decoupling database writes from external API calls to ensure reliable processing of Brevo operations.

Each row in the outbox is linked to a funnel entry via the `funnel_entry_id` field, which references `funnel_entries.id`. The table tracks operation status, retry attempts, and error information to support reliable delivery of Brevo API calls.

## 3. Conversion Report

To view funnel conversion, use the script:

```powershell
python -m app.report_conversions
```

Example output:

```text
Funnel conversion report
------------------------
language: entries=10, purchased=3, conversion=30.00%
non_language: entries=5, purchased=1, conversion=20.00%
```

Where:

* `entries` number of users who entered the funnel;
* `purchased` number of users who purchased a certificate after entering the funnel;
* `conversion` ratio `purchased / entries` as a percentage.

### 2.1. Date Filtering

You can specify a period:

```powershell
python -m app.report_conversions --from-date 2024-01-01 --to-date 2025-01-01
```

* `--from-date` inclusive;
* `--to-date` exclusive.

If only `--from-date` is specified, entries from that date to the current moment are considered.

If parameters are not specified, all entries from `funnel_entries` are taken.

## 4. Interpreting Results

Example questions that can be answered:

* How many people entered the language funnel in the last month?
* What is the conversion rate from funnel to certificate purchase for language tests?
* How does language test conversion differ from non-language tests?

For more detailed analytics, you can use SQL queries to `funnel_entries`, combining conditions by `email`, `user_id`, `test_id`, and time.

## 5. Extending Analytics with UTM Tags

Currently, the script does not modify email content, only sends contacts to Brevo. For extended analytics via UTM tags, you can use the following approach:

1. Add UTM tags to links in Brevo email templates, for example:

   * for language funnel:

     `?utm_source=testizer&utm_medium=email&utm_campaign=language_funnel`

   * for non-language:

     `?utm_source=testizer&utm_medium=email&utm_campaign=non_language_funnel`

2. Analyze clicks on these UTMs in analytics systems (e.g., Google Analytics or similar).

3. If needed, link external analytics with `funnel_entries` data by email and time periods.

With this approach, the database and script structure doesn't change, and extended analytics is configured through email templates and external reports.

## 6. Possible Development Directions

If more detailed analytics are needed in the future, you can:

* add a `source` field to `funnel_entries`, for example `email_language_v1`, `email_non_language_v1`;
* record funnel or campaign version there;
* build reports by combination of `funnel_type` + `source`.

The current architecture is already ready for such extensions, as:

* funnel entry is recorded centrally in `funnel_entries`;
* certificate purchase is linked to the same entries;
* the `app.report_conversions` report can be extended without changing the main business code.

