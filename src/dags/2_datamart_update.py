from airflow import DAG
from airflow.operators.python import PythonOperator
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

def create_dwh_tables(**kwargs):
    """Создает витрину в DWH (если она не существует)"""
    logging.info("Создаем витрину в DWH (если не существует)...")
    
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
            create_sql = load_sql_query('sql/dwh/create_global_metrics.sql')
            cursor.execute(create_sql)
            logging.info("Витрина global_metrics готова")
                
    except Exception as e:
        if "already exists" in str(e):
            logging.info("Витрина global_metrics уже существует - это нормально")
        else:
            logging.error(f"Ошибка при создании витрины: {str(e)}")
            raise
    finally:
        conn_vert.close()
    
    logging.info("Витрина в DWH готова к работе")

def update_global_metrics(**kwargs):
    """Обновляет витрину данных ТОЛЬКО за октябрь 2022"""
    execution_date = kwargs['execution_date']
    execution_date_naive = execution_date.replace(tzinfo=None)
    
    # Целевая дата = execution_date (для октября 2022)
    target_date_str = execution_date_naive.strftime('%Y-%m-%d')
    
    # Проверяем что дата входит в октябрь 2022
    if execution_date_naive < datetime(2022, 10, 1) or execution_date_naive > datetime(2022, 10, 31):
        logging.info(f"Пропускаем дату {target_date_str} - не входит в октябрь 2022")
        return
    
    logging.info(f"Обновляем витрину за {target_date_str}")
    
    try:
        sql_query = load_sql_query('sql/dwh/update_global_metrics.sql', target_date=target_date_str)
        logging.info(f"SQL запрос: {sql_query}")
        
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
            cursor.execute(sql_query)
            logging.info(f"Витрина успешно обновлена за {target_date_str}")
        
        conn_vert.close()
        
    except Exception as e:
        logging.error(f"Ошибка при обновлении витрины: {str(e)}")
        raise

with DAG(
    '2_datamart_update',
    default_args=default_args,
    description='Обновление витрины за октябрь 2022',
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    tags=['fintech', 'de_final'],
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_dwh_tables',
        python_callable=create_dwh_tables,
        provide_context=True
    )
    
    update_metrics_task = PythonOperator(
        task_id='update_global_metrics',
        python_callable=update_global_metrics,
        provide_context=True
    )

    create_tables_task >> update_metrics_task