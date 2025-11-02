from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import vertica_python
from py.sql_loader import load_sql_query


default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def create_staging_tables(**kwargs):
    """Создает таблицы в STAGING (если они не существуют)"""
    logging.info("Создаем таблицы в STAGING (если не существуют)...")
    
    vertica_conn = BaseHook.get_connection('vertica_de_final')
    conn_vert = vertica_python.connect(
        host=vertica_conn.host,
        port=vertica_conn.port,
        user=vertica_conn.login,
        password=vertica_conn.password,
        database='dwh',
        autocommit=True
    )
    
    try:
        with conn_vert.cursor() as cursor:
            create_sql = load_sql_query('sql/staging/create_transactions.sql')
            cursor.execute(create_sql)
            logging.info("Таблица transactions готова")
            
            create_sql = load_sql_query('sql/staging/create_currencies.sql')
            cursor.execute(create_sql)
            logging.info("Таблица currencies готова")
                
    except Exception as e:
        logging.error(f"Ошибка при создании таблиц: {str(e)}")
        raise
    finally:
        conn_vert.close()
    
    logging.info("Все таблицы в STAGING готовы к работе")

def load_transactions_to_staging(**kwargs):
    """Загружает транзакции за указанную дату execution_date (только октябрь 2022)"""
    execution_date = kwargs['execution_date']
    execution_date_naive = execution_date.replace(tzinfo=None)
    
    # Целевая дата = execution_date (для октября 2022)
    target_date_str = execution_date_naive.strftime('%Y-%m-%d')
    
    # Проверяем что дата входит в октябрь 2022
    if execution_date_naive < datetime(2022, 10, 1) or execution_date_naive > datetime(2022, 10, 31):
        logging.info(f"Пропускаем дату {target_date_str} - не входит в октябрь 2022")
        return
    
    logging.info(f"Загружаем транзакции за {target_date_str}")
    
    try:
        select_sql = load_sql_query('sql/staging/load_transactions.sql', target_date=target_date_str)
        logging.info(f"SQL запрос: {select_sql}")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_de_final')
        conn_pg = pg_hook.get_conn()
        
        with conn_pg.cursor() as cursor:
            cursor.execute(select_sql)
            transactions_data = cursor.fetchall()
        
        logging.info(f"Загрузили {len(transactions_data)} транзакций из PostgreSQL")
        
        vertica_conn = BaseHook.get_connection('vertica_de_final')
        conn_vert = vertica_python.connect(
            host=vertica_conn.host,
            port=vertica_conn.port,
            user=vertica_conn.login,
            password=vertica_conn.password,
            database='dwh',
            autocommit=True
        )
        
        with conn_vert.cursor() as cursor:
            delete_sql = load_sql_query('sql/staging/delete_transactions.sql', target_date=target_date_str)
            cursor.execute(delete_sql)
            
            insert_sql = load_sql_query('sql/staging/insert_transactions.sql')
            if transactions_data:
                cursor.executemany(insert_sql, transactions_data)
                logging.info(f"Успешно вставили {len(transactions_data)} записей в Vertica")
            else:
                logging.warning(f"Нет данных для вставки за {target_date_str}")
        
        conn_pg.close()
        conn_vert.close()
        logging.info(f"Успешно загрузили {len(transactions_data)} транзакций в Vertica")
        
    except Exception as e:
        logging.error(f"Ошибка при загрузке транзакций: {str(e)}")
        raise

def load_currencies_to_staging(**kwargs):
    """Загружает курсы валют за указанную дату execution_date (только октябрь 2022)"""
    execution_date = kwargs['execution_date']
    execution_date_naive = execution_date.replace(tzinfo=None)
    
    # Целевая дата = execution_date (для октября 2022)
    target_date_str = execution_date_naive.strftime('%Y-%m-%d')
    
    # Проверяем что дата входит в октябрь 2022
    if execution_date_naive < datetime(2022, 10, 1) or execution_date_naive > datetime(2022, 10, 31):
        logging.info(f"Пропускаем дату {target_date_str} - не входит в октябрь 2022")
        return
    
    logging.info(f"Загружаем курсы валют за {target_date_str}")
    
    try:
        select_sql = load_sql_query('sql/staging/load_currencies.sql', target_date=target_date_str)
        logging.info(f"SQL запрос: {select_sql}")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_de_final')
        conn_pg = pg_hook.get_conn()
        
        with conn_pg.cursor() as cursor:
            cursor.execute(select_sql)
            currencies_data = cursor.fetchall()
        
        logging.info(f"Загрузили {len(currencies_data)} курсов валют из PostgreSQL")
        
        vertica_conn = BaseHook.get_connection('vertica_de_final')
        conn_vert = vertica_python.connect(
            host=vertica_conn.host,
            port=vertica_conn.port,
            user=vertica_conn.login,
            password=vertica_conn.password,
            database='dwh',
            autocommit=True
        )
        
        with conn_vert.cursor() as cursor:
            delete_sql = load_sql_query('sql/staging/delete_currencies.sql', target_date=target_date_str)
            cursor.execute(delete_sql)
            
            insert_sql = load_sql_query('sql/staging/insert_currencies.sql')
            if currencies_data:
                cursor.executemany(insert_sql, currencies_data)
                logging.info(f"Успешно вставили {len(currencies_data)} записей в Vertica")
            else:
                logging.warning(f"Нет данных для вставки за {target_date_str}")
        
        conn_pg.close()
        conn_vert.close()
        logging.info(f"Успешно загрузили {len(currencies_data)} курсов валют в Vertica")
        
    except Exception as e:
        logging.error(f"Ошибка при загрузке курсов валют: {str(e)}")
        raise

with DAG(
    '1_data_import',
    default_args=default_args,
    description='Загрузка данных в STAGING за октябрь 2022',
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    tags=['fintech', 'de_final'],
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
        provide_context=True
    )
    
    load_transactions_task = PythonOperator(
        task_id='load_transactions',
        python_callable=load_transactions_to_staging,
        provide_context=True
    )
    
    load_currencies_task = PythonOperator(
        task_id='load_currencies',
        python_callable=load_currencies_to_staging,
        provide_context=True   

    )

    create_tables_task >> [load_transactions_task, load_currencies_task]