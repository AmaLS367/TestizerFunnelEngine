# Testizer Email Funnels

Python service for managing email funnels for Testizer.com based on MySQL data and Brevo.

On Stage 1 the project provides:
- project structure,
- configuration loading from `.env` and environment variables,
- logging to console and file,
- MySQL connection,
- placeholder selectors and a main entrypoint for a periodic job.

## Setup on Windows

```powershell
cd PATH\TO\testizer_email_funnels
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# Then edit .env with real database credentials
```

## Run Stage 1 job

```powershell
.\.venv\Scripts\Activate.ps1
python -m app.main
```

Logs will be written to `logs/app.log`.
