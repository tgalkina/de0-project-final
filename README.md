# DWH для финтех-стартапа

**Стек:** Python, Airflow, PostgreSQL, Vertica

## О проекте
ETL-пайплайн для анализа транзакций пользователей из разных стран. Данные выгружаются из PostgreSQL, обрабатываются в Vertica и формируют витрину с метриками по переводам.

## Структура
- `dags/` — 2 DAG'a: загрузка в staging и обновление витрины
- `sql/` — DDL и скрипты расчёта метрик
- `py/` — вспомогательные функции

## Витрина global_metrics
- `date_update` — дата расчёта
- `currency_from` — код валюты
- `amount_total` — сумма в долларах
- `cnt_transactions` — количество транзакций
- `avg_transactions_per_account` — среднее число транзакций на аккаунт
- `cnt_accounts_make_transactions` — количество уникальных аккаунтов

## Как запустить
1. Создать Connections в Airflow: `postgres_de_final`, `vertica_de_final`
2. Запустить DAG'ы: `1_data_import` → `2_datamart_update`
