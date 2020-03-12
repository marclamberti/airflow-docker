from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

from datetime import datetime, timedelta
import requests
import airflow
import json
import csv

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'owner': 'airflow'
}

class CustomS3ToRedshiftTransfer(S3ToRedshiftTransfer):
    template_fields = ('s3_bucket', 's3_key')


def store_csv_to_s3(**context):
    filename = 'data_' + context['execution_date'].to_date_string() + '.csv'
    S3Hook(
        aws_conn_id='s3-flight'
    ).load_file(
        filename='/usr/local/airflow/data/' + filename ,
        key=context['execution_date'].to_date_string() + "/flights",
        bucket_name=Variable.get('bucket_name'),
        replace=True
    )

def json_to_csv(**context):
    with open('/usr/local/airflow/data/data_' + context['execution_date'].to_date_string() + '.json') as inf:
        data = json.load(inf)['data']
        with open('/usr/local/airflow/data/data_' + context['execution_date'].to_date_string() + '.csv', 'w') as ouf:
            f = csv.writer(ouf, quoting=csv.QUOTE_NONE)
            f.writerow([
                "flight_number",
                "flight_date",
                "flight_status",
                "departure_airport",
                "departure_icao",
                "arrival_airport",
                "arrival_icao",
                "airline_name",
                "aircraft_icao",
                "live_updated",
                "live_latitude",
                "live_longitude"
            ])
            for row in data:
                if not row['aircraft'] or not row['live']:
                    continue
                f.writerow([
                    row['flight']['number'],
                    row['flight_date'],
                    row['flight_status'],
                    row['departure']['airport'],
                    row['departure']['icao'],
                    row['arrival']['airport'],
                    row['arrival']['icao'],
                    row['airline']['name'],
                    row['aircraft']['icao'],
                    row['live']['updated'],
                    row['live']['latitude'],
                    row['live']['longitude']
            ])

def getting_api_data(**context):
    r = requests.get("http://api.aviationstack.com/v1/flights?access_key=" + Variable.get("flight_secret_key") + "&flight_status=active")
    data = r.json()
    with open('/usr/local/airflow/data/data_' + context['execution_date'].to_date_string() + ".json", 'w') as f:
        json.dump(data, f, ensure_ascii=False)

with DAG(dag_id='flight_pipeline', schedule_interval="*/2 * * * *", default_args=default_args) as dag:

    # Task 1: Getting API data
    task_1 = PythonOperator(
        task_id='getting_api_data',
        python_callable=getting_api_data,
        provide_context=True
        )
    
    # Task 2: Json to CSV
    task_2 = PythonOperator(
        task_id='json_to_csv',
        python_callable=json_to_csv,
        provide_context=True
        )
    
    # Task 3: Store csv to AWS S3
    task_3 = PythonOperator(
        task_id='store_csv_to_s3',
        python_callable=store_csv_to_s3,
        provide_context=True
    )

    # Task 4: S3 to Redshift
    # /!\ - Values in csv must match with the order of table columns
    #     - Change the default delimiter of Redshift according to the one in your csv
    #     - In the connection from AIRFLOW UI
    #       - The schema is the DBName
    #       - The host must be provided (endpoint of Redshift)
    task_4 = CustomS3ToRedshiftTransfer(
        task_id='load_flights_into_refshift',
        schema='public',
        table='flights',
        s3_bucket='{{ var.value.bucket_name }}',
        s3_key='{{ ds }}',
        redshift_conn_id='redshift-flight',
        aws_conn_id='s3-flight',
        copy_options=("IGNOREHEADER 1", "DELIMITER ','")
    )