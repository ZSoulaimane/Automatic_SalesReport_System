from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from datacleaner import data_cleaner


default_args = {
            'owner': 'Airflow',
            'start_date': datetime(2019, 9, 12),
            'retries': 1,
            'retry_delay': timedelta(seconds=5)
}


with DAG(dag_id='Workflow', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    check_file = BashOperator(
        task_id='check_file',
        bash_command='shasum ~/ip_files/raw_store_transactions.csv',
        retries = 2,
        retry_delay=timedelta(seconds=15)
    )

    data_cleaning = PythonOperator(
        task_id = "cleaning",
        python_callable = data_cleaner
    )

    create_table = MySqlOperator(
        task_id = "create_table",
        mysql_conn_id = "mysql_conn",
        sql = "CREATE TABLE IF NOT EXISTS clean_store(STORE_ID varchar(50), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DATE date);"
    )

    load_data = MySqlOperator(
        task_id = "load_data",
        mysql_conn_id = "mysql_conn",
        sql = "LOAD DATA INFILE '/var/lib/mysql-files/fin.csv' INTO TABLE clean_store FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"
    )

    email = EmailOperator(task_id='send_email',
        to='soulaimane.studies@gmail.com',
        subject='Daily report',
        html_content=""" <h1>Congrats! Your report is ready.</h1> """,
        )

    check_file >> data_cleaning >> create_table >> load_data >> email