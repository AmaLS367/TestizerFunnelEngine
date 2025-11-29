# Testizer Email Funnels

Python-сервис для управления email-воронками Testizer.com на основе данных MySQL и API Brevo.

## Возможности

- **Автоматическое управление воронками**: Автоматически определяет кандидатов по тестам и добавляет их в email-маркетинговые воронки
- **Интеграция с Brevo**: Синхронизация с Brevo (бывший Sendinblue) для управления контактами
- **Отслеживание покупок**: Мониторинг покупок сертификатов и обновление аналитики воронок
- **Аналитика конверсии**: Встроенные инструменты отчетности для анализа эффективности воронок
- **Идемпотентная обработка**: Безопасный многократный запуск без создания дубликатов
- **Режим Dry-Run**: Тестирование функциональности без реальных API-вызовов

## Быстрый старт

### Установка на Windows

```powershell
cd PATH\TO\testizer_email_funnels
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# Затем отредактируйте .env с реальными учетными данными БД и API-ключом Brevo
```

### Запуск основной задачи

```powershell
.\.venv\Scripts\Activate.ps1
python -m app.main
```

### Генерация отчета по конверсии

```powershell
python -m app.report_conversions
python -m app.report_conversions --from-date 2024-01-01 --to-date 2025-01-01
```

Логи записываются в `logs/app.log`.

## Документация

Полная документация доступна в директории `docs/`:

- **[Руководство по эксплуатации](docs/operations_guide.md)** - Развертывание в продакшене, переменные окружения, ручной запуск и настройка планировщика задач Windows
- **[Руководство по аналитике](docs/analytics_guide.md)** - Понимание метрик конверсии, чтение отчетов и интерпретация результатов
- **[Руководство по доставляемости email](docs/deliverability.md)** - Лучшие практики доставляемости, настройка SPF/DKIM и конфигурация Brevo
- **[Руководство по расписанию](docs/scheduling.md)** - Рекомендуемые частоты запуска, стратегии пакетной обработки и настройка автоматизации
- **[Схема базы данных](docs/db_analytics_schema.sql)** - SQL-схема таблицы аналитики `funnel_entries`

## Структура проекта

```
testizer_email_funnels/
├── app/                    # Точки входа приложения
│   ├── main.py            # Основная задача синхронизации
│   └── report_conversions.py  # CLI для отчетов по конверсии
├── analytics/              # Аналитика и отслеживание
│   ├── tracking.py        # Управление записями воронок
│   ├── reports.py         # Генерация отчетов по конверсии
│   └── report_service.py  # Сервисный слой отчетов
├── brevo/                  # Интеграция с Brevo API
│   ├── api_client.py      # HTTP-клиент для Brevo API
│   └── models.py          # Модели данных Brevo
├── config/                 # Управление конфигурацией
│   └── settings.py         # Загрузка настроек из .env
├── db/                     # Слой базы данных
│   ├── connection.py      # Управление подключениями MySQL
│   └── selectors.py       # Функции запросов к БД
├── funnels/                # Бизнес-логика воронок
│   ├── models.py          # Доменные модели
│   ├── sync_service.py    # Сервис синхронизации воронок
│   └── purchase_sync_service.py  # Сервис отслеживания покупок
├── logging_config/         # Конфигурация логирования
│   └── logger.py          # Настройка логирования
└── docs/                   # Документация
```

## Требования

- Python 3.10+
- База данных MySQL (MODX)
- Аккаунт Brevo API
- См. `requirements.txt` для зависимостей Python

## Переменные окружения

Ключевые переменные конфигурации (см. `.env.example` для полного списка):

- `APP_ENV` - Окружение: `development` или `production`
- `APP_DRY_RUN` - Установите `true` для тестирования без API-вызовов
- `DB_*` - Параметры подключения к MySQL
- `BREVO_API_KEY` - API-ключ Brevo
- `BREVO_LANGUAGE_LIST_ID` - ID списка Brevo для воронки языковых тестов
- `BREVO_NON_LANGUAGE_LIST_ID` - ID списка Brevo для воронки неязыковых тестов

Подробные инструкции по настройке см. в [Руководстве по эксплуатации](docs/operations_guide.md).

## Лицензия

MIT License - см. файл [LICENSE](LICENSE) для деталей.
