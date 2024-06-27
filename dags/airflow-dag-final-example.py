from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import csv

def fetch_data():
    # Simulação de uma requisição HTTP (exemplo fictício)
    data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'},
        {'id': 3, 'name': 'Charlie'}
    ]

    return data

def process_data(data):
    # Processamento dos dados recebidos (simulação simples)
    with open('/opt/airflow/dags/http_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['id', 'name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in data:
            writer.writerow(item)

# Definindo a DAG
with DAG('http_request_example',
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    # Tarefa para simular a obtenção de dados via HTTP
    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data,
    )

    # Tarefa para processar os dados recebidos e gerar o arquivo CSV
    process_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        op_args=[fetch_task.output],  # Passa o output do fetch_task como argumento
    )

    # Definindo dependências
    fetch_task >> process_task
