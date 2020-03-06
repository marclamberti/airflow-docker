from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import requests
import airflow
import json
import csv

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'owner': 'airflow'
}

def json_to_csv(**context):
    with open('/usr/local/airflow/data/data_' + context['execution_date'].to_date_string() + '.json') as inf:
        data = json.load(inf)['data']
        with open('/usr/local/airflow/data/data_' + context['execution_date'].to_date_string() + '.csv', 'w') as ouf:
            f = csv.writer(ouf)
            f.writerow([
                "flight_date",
                "flight_status",
                "departure.airport",
                "departure.icao",
                "arrival.airport",
                "arrival.icao",
                "airline.name",
                "flight.number",
                "aircraft.icao",
                "live.updated",
                "live.latitude",
                "live.longitude"
            ])
            f.writerow([
                data['flight_date'],
                data['flight_status'],
                data['departure']['airport'],
                data['departure']['icao'],
                data['arrival']['airport'],
                data['arrival']['icao'],
                data['airline']['name'],
                data['flight']['number'],
                data['aircraft']['icao'],
                data['live']['updated'],
                data['live']['latitude'],
                data['live']['longitude']
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
    
    # Task 3: Store data to Redshift tables
