# Testizer Email Funnels

[![CI](https://github.com/AmaLS367/TestizerFunnelEngine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AmaLS367/TestizerFunnelEngine/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/badge/linting-ruff-3f8cff?logo=ruff&logoColor=white)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

**Languages:** **English** | [Русский](README.ru.md)

Python service for managing email funnels for Testizer.com based on MySQL data and Brevo API integration.

## Features

- **Automated Funnel Management**: Automatically identifies test candidates and adds them to email funnels
- **Brevo Integration**: Seamless integration with Brevo API for contact management
- **Purchase Tracking**: Tracks certificate purchases and updates funnel analytics
- **Conversion Analytics**: Built-in reporting system for funnel conversion metrics
- **Dry Run Mode**: Safe testing without affecting production data
- **Docker Support**: Containerized deployment ready
- **Comprehensive Testing**: Full test coverage with pytest

## Quick Start

### Prerequisites

- Python 3.11 or higher
- MySQL database access (MODX)
- Brevo API key
- Virtual environment (recommended)

### Installation

1. Clone the repository:

```bash
git clone git@github.com:AmaLS367/testizer_email_funnels.git
cd testizer_email_funnels
```

2. Create and activate virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. Install dependencies:

```powershell
pip install -r requirements.txt
```

4. Configure environment:

```powershell
Copy-Item .env.example .env
# Edit .env with your database and Brevo credentials
```

5. Run the service:

```powershell
python -m app.main
```

## Docker

The service can be run in Docker for easier deployment and consistency across environments.

### Building the Image

```powershell
docker build -t testizer-funnel-engine .
```

### Running the Container

```powershell
docker run --rm --env-file .env testizer-funnel-engine
```

For detailed Docker usage instructions, see:
- [Docker Guide (Russian)](docs/ru/docker_guide.md)
- [Docker Guide (English)](docs/en/docker_guide.md)

## Project Structure

```
testizer_email_funnels/
├── app/                    # Main application entry points
│   ├── main.py            # Primary job orchestrator
│   └── report_conversions.py  # Conversion reporting CLI
├── analytics/             # Analytics and tracking
│   ├── tracking.py        # Funnel entry management
│   ├── reports.py         # Report generation
│   └── report_service.py # Report service layer
├── brevo/                 # Brevo API integration
│   ├── api_client.py      # HTTP client for Brevo API
│   └── models.py         # Brevo data models
├── config/                # Configuration management
│   └── settings.py        # Settings loading from environment
├── db/                    # Database layer
│   ├── connection.py     # MySQL connection management
│   └── selectors.py      # Database queries
├── funnels/               # Funnel business logic
│   ├── models.py         # Domain models
│   ├── sync_service.py   # Funnel synchronization
│   └── purchase_sync_service.py  # Purchase tracking
├── logging_config/        # Logging setup
│   └── logger.py         # Logging configuration
├── tests/                 # Test suite
│   ├── analytics/        # Analytics tests
│   ├── app/             # Application tests
│   ├── brevo/           # Brevo integration tests
│   ├── config/          # Configuration tests
│   ├── db/              # Database tests
│   ├── funnels/         # Funnel tests
│   └── logging_config/   # Logging tests
├── docs/                  # Documentation
│   ├── ru/              # Russian documentation
│   ├── en/              # English documentation
│   └── db_analytics_schema.sql  # Database schema
├── Dockerfile            # Docker image definition
├── .dockerignore        # Docker ignore patterns
├── .flake8              # Flake8 configuration (deprecated, using Ruff)
├── pyproject.toml       # Project configuration (black, mypy, pytest)
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Requirements

### Python Dependencies

- `mysql-connector-python==9.1.0` - MySQL database connectivity
- `python-dotenv==1.0.1` - Environment variable management
- `requests==2.32.3` - HTTP client for Brevo API
- `pytest==8.3.4` - Testing framework
- `sentry-sdk==2.19.1` - Error tracking
- `ruff` - Fast Python linter (replaces flake8)
- `mypy==1.19.0` - Type checking
- `black==25.11.0` - Code formatting

## Environment Variables

Required variables in `.env`:

```env
# Application
APP_ENV=development
APP_DRY_RUN=true
APP_LOG_LEVEL=INFO

# Database
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=testizer_user
DB_PASSWORD=change_me
DB_NAME=testizer
DB_CHARSET=utf8mb4

# Brevo
BREVO_API_KEY=your_api_key_here
BREVO_BASE_URL=https://api.brevo.com/v3
BREVO_LANGUAGE_LIST_ID=0
BREVO_NON_LANGUAGE_LIST_ID=0

# Sentry (optional)
SENTRY_DSN=your_sentry_dsn_here
```

See `.env.example` for the complete template.

## Documentation

### Russian (Русский)

- [Operations Guide](docs/ru/operations_guide.md) - Production deployment and operations
- [Analytics Guide](docs/ru/analytics_guide.md) - Conversion metrics and reporting
- [Docker Guide](docs/ru/docker_guide.md) - Docker deployment instructions
- [Deliverability Guide](docs/ru/deliverability.md) - Email deliverability best practices
- [Scheduling Guide](docs/ru/scheduling.md) - Task scheduling recommendations

### English

- [Operations Guide](docs/en/operations_guide.md) - Production deployment and operations
- [Analytics Guide](docs/en/analytics_guide.md) - Conversion metrics and reporting
- [Docker Guide](docs/en/docker_guide.md) - Docker deployment instructions
- [Deliverability Guide](docs/en/deliverability.md) - Email deliverability best practices
- [Scheduling Guide](docs/en/scheduling.md) - Task scheduling recommendations

### Database Schema

- [Analytics Schema](docs/db_analytics_schema.sql) - `funnel_entries` table definition

## Testing

The project includes comprehensive test coverage using pytest.

### Running Tests

```powershell
python -m pytest
```

### Test Coverage

Tests are organized by module:

- **Analytics**: Funnel tracking and conversion reporting
- **Application**: Main entry points and CLI tools
- **Brevo**: API client and models
- **Configuration**: Settings loading
- **Database**: Connection and query tests
- **Funnels**: Synchronization and purchase tracking
- **Logging**: Configuration tests

### Code Quality

The project uses several tools to maintain code quality:

- **Black**: Automatic code formatting
- **Ruff**: Fast Python linter
- **MyPy**: Static type checking
- **Pytest**: Test execution

Run all checks:

```powershell
python -m ruff check .
python -m black . --check
python -m mypy .
python -m pytest -m "not integration"
```

## CI/CD

Continuous Integration is configured via GitHub Actions:

- Runs on push and pull requests to `main`
- Tests on Ubuntu and Windows
- Python 3.11 and 3.12
- Checks code formatting, style, types, and runs tests

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml) for details.

## Development

### Code Style

The project follows PEP 8 with Black formatting (88 character line length). Configuration is in `pyproject.toml`.

### Type Hints

All code uses type hints for better maintainability and IDE support. MyPy is used for type checking.

### Commits

Follow conventional commit format:
- `feat/`: New features
- `fix/`: Bug fixes
- `docs/`: Documentation changes
- `test/`: Test additions/changes
- `chore/`: Maintenance tasks
- `ops/`: Operations/infrastructure

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Ensure all tests pass and code quality checks succeed
5. Submit a pull request

## Support

For issues and questions, please open an issue on GitHub.
