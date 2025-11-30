# Testizer email funnels · Operations guide

This document describes how to run and maintain the Python funnel script for the Testizer website.

## 1. Environment variables

Main variables in the `.env` file:

- `APP_ENV`

  Value: `development` or `production`.

- `APP_DRY_RUN`

  Value: `true` or `false`.

  When `APP_DRY_RUN=true`:

  * The service executes all read queries to the source database (MODX tables).
  * No changes are made to the `funnel_entries` table.
  * No real requests are sent to Brevo (only logs are produced).

  When `APP_DRY_RUN=false`, the service performs real writes to both the database and Brevo.

  Dry-run mode is intended for safe validation on production data without modifying state. This allows operators to verify query logic, data selection, and expected operations before enabling actual writes.

  For production mode, set to `false`.

- `APP_LOG_LEVEL`

  Possible values: `DEBUG`, `INFO`, `WARNING`, `ERROR`.

  Recommended value for production: `INFO`.

- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_CHARSET`

  MODX database connection parameters.

- `BREVO_API_KEY`

  API key from the Brevo dashboard.

- `BREVO_BASE_URL`

  Brevo API base URL. Default is `https://api.brevo.com/v3`.

- `BREVO_LANGUAGE_LIST_ID`

  Brevo list ID for language tests.

- `BREVO_NON_LANGUAGE_LIST_ID`

  Brevo list ID for non-language tests.

Before enabling production mode, it's important to verify that:

1. Database connection works.
2. Correct lists exist in Brevo and their IDs are recorded in `.env`.
3. `APP_DRY_RUN` is temporarily set to `true` for test runs.

## 2. Manual Task Execution

Work is performed from the project root in an activated virtual environment.

### 2.1. Activating the Environment

```powershell
cd C:\Users\user\Desktop\testizer_email_funnels
.\.venv\Scripts\Activate.ps1
```

### 2.2. Running the Main Task

```powershell
python -m app.main
```

This will:

* select candidates for language and non-language tests;
* send contacts to Brevo (or log dry-run message, depending on `APP_DRY_RUN`);
* record funnel entry in the `funnel_entries` table (or log dry-run message, depending on `APP_DRY_RUN`);
* sync certificate purchases with MODX tables and update `funnel_entries` (or log dry-run message, depending on `APP_DRY_RUN`).

### 2.3. Running Conversion Report

```powershell
python -m app.report_conversions
```

or with a date range:

```powershell
python -m app.report_conversions --from-date 2024-01-01 --to-date 2025-01-01
```

Dates are specified in `YYYY-MM-DD` format.

If `--to-date` is not specified, entries up to the current moment are considered.

## 3. Transitioning from Dry Run to Production Mode

Recommended sequence:

1. Keep `APP_DRY_RUN=true`.

2. Run several times:

   ```powershell
   python -m app.main
   ```

   and check logs in `logs/app.log`, as well as logs in Brevo, to verify requests are formed correctly.

3. Verify that the logs show what would be written to the `funnel_entries` table and everything looks as expected. Note: In dry-run mode, no entries are actually written to the database.

4. Change in `.env`:

   ```text
   APP_ENV=production
   APP_DRY_RUN=false
   ```

5. Restart the task manually and verify that:

   * real contacts appear in Brevo in the correct lists;
   * the `funnel_entries` table is populated;
   * the `app.report_conversions` report returns funnel data.

## 4. Setting Up Periodic Execution on Windows (Task Scheduler)

This section describes setting up scheduling on Windows for a test environment or local setup.

1. Open "Task Scheduler".

2. Create a new task:

   * "Create Task".

   * Set a name, for example: `Testizer email funnels job`.

3. Triggers tab:

   * "Create".

   * Select schedule, for example "On a schedule" → "Hourly" or desired interval.

   * Save the trigger.

4. Actions tab:

   * "Create".

   * Action: "Start a program".

   * In the "Program or script" field, specify the path to `powershell.exe`, for example:

     ```text
     C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
     ```

   * In the "Add arguments" field, specify:

     ```text
     -ExecutionPolicy Bypass -File "C:\Users\user\Desktop\testizer_email_funnels\run_job.ps1"
     ```

5. Conditions and Settings tabs:

   * Configure as needed (can leave default values).

6. Save the task.

## 5. run_job.ps1 Script

For convenience, you can extract Python execution into a separate PowerShell script `run_job.ps1` in the project root:

```powershell
$projectPath = "C:\Users\user\Desktop\testizer_email_funnels"
Set-Location $projectPath
.\.venv\Scripts\Activate.ps1
python -m app.main
```

This file should be specified in the scheduler arguments.

## 6. Monitoring Recommendations

* Check the `logs/app.log` file for errors.

* Periodically run:

  ```powershell
  python -m app.report_conversions
  ```

  to track funnel effectiveness for language and non-language tests.

