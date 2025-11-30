# Testizer Email Funnels

![CI](https://github.com/AmaLS367/testizer_email_funnels/actions/workflows/ci.yml/badge.svg?branch=main)
![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)

**Языки:** [English](README.md) | **Русский**

Python-сервис для управления email-воронками Testizer.com на основе данных MySQL и интеграции с API Brevo.

## Возможности

- **Автоматическое управление воронками**: Автоматически определяет кандидатов по тестам и добавляет их в email-маркетинговые воронки
- **Интеграция с Brevo**: Бесшовная интеграция с API Brevo для управления контактами
- **Отслеживание покупок**: Отслеживает покупки сертификатов и обновляет аналитику воронок
- **Аналитика конверсии**: Встроенная система отчетности для метрик конверсии воронок
- **Режим Dry Run**: Безопасное тестирование без влияния на продакшн-данные
- **Поддержка Docker**: Готов к контейнеризованному развертыванию
- **Комплексное тестирование**: Полное покрытие тестами с pytest

## Быстрый старт

### Требования

- Python 3.10 или выше
- Доступ к базе данных MySQL (MODX)
- API-ключ Brevo
- Виртуальное окружение (рекомендуется)

### Установка

1. Клонируйте репозиторий:

```bash
git clone git@github.com:AmaLS367/testizer_email_funnels.git
cd testizer_email_funnels
```

2. Создайте и активируйте виртуальное окружение:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. Установите зависимости:

```powershell
pip install -r requirements.txt
```

4. Настройте окружение:

```powershell
Copy-Item .env.example .env
# Отредактируйте .env с вашими учетными данными БД и Brevo
```

5. Запустите сервис:

```powershell
python -m app.main
```

## Docker

Сервис может быть запущен в Docker для более простого развертывания и согласованности между окружениями.

### Сборка образа

```powershell
docker build -t testizer-funnel-engine .
```

### Запуск контейнера

```powershell
docker run --rm --env-file .env testizer-funnel-engine
```

Подробные инструкции по использованию Docker см.:
- [Руководство по Docker (Русский)](docs/ru/docker_guide.md)
- [Docker Guide (English)](docs/en/docker_guide.md)

## Структура проекта

```
testizer_email_funnels/
├── app/                    # Точки входа приложения
│   ├── main.py            # Основной оркестратор задач
│   └── report_conversions.py  # CLI для отчетов по конверсии
├── analytics/             # Аналитика и отслеживание
│   ├── tracking.py        # Управление записями воронок
│   ├── reports.py         # Генерация отчетов
│   └── report_service.py # Сервисный слой отчетов
├── brevo/                 # Интеграция с API Brevo
│   ├── api_client.py      # HTTP-клиент для API Brevo
│   └── models.py         # Модели данных Brevo
├── config/                # Управление конфигурацией
│   └── settings.py        # Загрузка настроек из окружения
├── db/                    # Слой базы данных
│   ├── connection.py     # Управление подключениями MySQL
│   └── selectors.py      # Запросы к базе данных
├── funnels/               # Бизнес-логика воронок
│   ├── models.py         # Доменные модели
│   ├── sync_service.py   # Синхронизация воронок
│   └── purchase_sync_service.py  # Отслеживание покупок
├── logging_config/        # Настройка логирования
│   └── logger.py         # Конфигурация логирования
├── tests/                 # Набор тестов
│   ├── analytics/        # Тесты аналитики
│   ├── app/             # Тесты приложения
│   ├── brevo/           # Тесты интеграции Brevo
│   ├── config/          # Тесты конфигурации
│   ├── db/              # Тесты базы данных
│   ├── funnels/         # Тесты воронок
│   └── logging_config/   # Тесты логирования
├── docs/                  # Документация
│   ├── ru/              # Русская документация
│   ├── en/              # Английская документация
│   └── db_analytics_schema.sql  # Схема базы данных
├── Dockerfile            # Определение Docker-образа
├── .dockerignore        # Паттерны игнорирования для Docker
├── .flake8              # Конфигурация Flake8
├── pyproject.toml       # Конфигурация проекта (black, mypy, pytest)
├── requirements.txt     # Зависимости Python
└── README.md           # Этот файл
```

## Требования

### Зависимости Python

- `mysql-connector-python==9.1.0` - Подключение к базе данных MySQL
- `python-dotenv==1.0.1` - Управление переменными окружения
- `requests==2.32.3` - HTTP-клиент для API Brevo
- `pytest==8.3.4` - Фреймворк тестирования
- `sentry-sdk==2.19.1` - Отслеживание ошибок
- `flake8==7.3.0` - Проверка стиля кода
- `mypy==1.19.0` - Проверка типов
- `black==25.11.0` - Форматирование кода

## Переменные окружения

Необходимые переменные в `.env`:

```env
# Приложение
APP_ENV=development
APP_DRY_RUN=true
APP_LOG_LEVEL=INFO

# База данных
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

# Sentry (опционально)
SENTRY_DSN=your_sentry_dsn_here
```

См. `.env.example` для полного шаблона.

## Документация

### Русский

- [Руководство по эксплуатации](docs/ru/operations_guide.md) - Развертывание в продакшене и эксплуатация
- [Руководство по аналитике](docs/ru/analytics_guide.md) - Метрики конверсии и отчетность
- [Руководство по Docker](docs/ru/docker_guide.md) - Инструкции по развертыванию Docker
- [Руководство по доставляемости](docs/ru/deliverability.md) - Лучшие практики доставляемости email
- [Руководство по расписанию](docs/ru/scheduling.md) - Рекомендации по планированию задач

### English

- [Operations Guide](docs/en/operations_guide.md) - Production deployment and operations
- [Analytics Guide](docs/en/analytics_guide.md) - Conversion metrics and reporting
- [Docker Guide](docs/en/docker_guide.md) - Docker deployment instructions
- [Deliverability Guide](docs/en/deliverability.md) - Email deliverability best practices
- [Scheduling Guide](docs/en/scheduling.md) - Task scheduling recommendations

### Схема базы данных

- [Схема аналитики](docs/db_analytics_schema.sql) - Определение таблицы `funnel_entries`

## Тестирование

Проект включает комплексное покрытие тестами с использованием pytest.

### Запуск тестов

```powershell
python -m pytest
```

### Покрытие тестами

Тесты организованы по модулям:

- **Analytics**: Отслеживание воронок и отчеты по конверсии
- **Application**: Основные точки входа и CLI-инструменты
- **Brevo**: API-клиент и модели
- **Configuration**: Загрузка настроек
- **Database**: Тесты подключений и запросов
- **Funnels**: Синхронизация и отслеживание покупок
- **Logging**: Тесты конфигурации

### Качество кода

Проект использует несколько инструментов для поддержания качества кода:

- **Black**: Автоматическое форматирование кода
- **Flake8**: Проверка соответствия стилю
- **MyPy**: Статическая проверка типов
- **Pytest**: Выполнение тестов

Запуск всех проверок:

```powershell
black --check .
flake8 .
mypy .
pytest
```

## CI/CD

Непрерывная интеграция настроена через GitHub Actions:

- Запускается при push и pull request в `main`
- Тестирование на Ubuntu и Windows
- Python 3.13
- Проверка форматирования кода, стиля, типов и запуск тестов

См. [`.github/workflows/ci.yml`](.github/workflows/ci.yml) для деталей.

## Разработка

### Стиль кода

Проект следует PEP 8 с форматированием Black (длина строки 88 символов). Конфигурация находится в `pyproject.toml`.

### Аннотации типов

Весь код использует аннотации типов для лучшей поддерживаемости и поддержки IDE. MyPy используется для проверки типов.

### Коммиты

Следуйте формату conventional commits:
- `feat/`: Новые функции
- `fix/`: Исправления ошибок
- `docs/`: Изменения документации
- `test/`: Добавление/изменение тестов
- `chore/`: Задачи по обслуживанию
- `ops/`: Операции/инфраструктура

## Лицензия

MIT License - см. файл [LICENSE](LICENSE) для деталей.

## Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для функции
3. Внесите изменения
4. Убедитесь, что все тесты проходят и проверки качества кода успешны
5. Отправьте pull request

## Поддержка

По вопросам и проблемам, пожалуйста, создайте issue на GitHub.

