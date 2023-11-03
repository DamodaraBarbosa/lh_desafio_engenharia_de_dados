from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('/home/damodarabarbosa/airflow_tooltorial/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("/home/damodarabarbosa/airflow_tooltorial/final_output.txt","w") as f:
        f.write(base64_message)
    return None

## Do not change the code above this line-----------------------##

def extract_from_northwind(**context):
    """
    Extrai a tabela Order do banco Northwind.
    """
    conn = sqlite3.connect("/home/damodarabarbosa/airflow_tooltorial/data/Northwind_small.sqlite")
    query = "SELECT * FROM 'Order';"
    df_order = pd.read_sql_query(query, conn)
    df_order.to_csv("/home/damodarabarbosa/airflow_tooltorial/data/output_orders.csv")
    conn.close()

def export_count_file(**context):
    """
    Extrai a tabela OrderDetail e faz o merge com a tabela Order. Por fim faz a contagem da 
    quantidade de produtos vendidos com destino ao Rio de Janeiro. A contagem é exportada no
    arquivo count.txt.
    """

    conn = sqlite3.connect("/home/damodarabarbosa/airflow_tooltorial/data/Northwind_small.sqlite")
    query = "SELECT * FROM OrderDetail;"
    order_details = pd.read_sql_query(query, conn)
    order = pd.read_csv("/home/damodarabarbosa/airflow_tooltorial/data/output_orders.csv")
    df_merged = pd.merge(
        left=order_details, right=order, how="inner",
        left_on="OrderId", right_on="Id"
    )
    ship_rio_de_janeiro = df_merged.query("ShipCity == 'Rio de Janeiro'")
    count_quantity = str(ship_rio_de_janeiro["Quantity"].sum())
    with open("/home/damodarabarbosa/airflow_tooltorial/count.txt", "w") as file:
        file.write(count_quantity)
    conn.close()

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id="export_final_output",
        python_callable=export_final_answer,
        provide_context=True
    )

    extract_order_from_northwind = PythonOperator(
        task_id="extract_order_from_northwind",
        python_callable=extract_from_northwind,
        provide_context=True
    )

    export_count_text = PythonOperator(
        task_id="export_count_text",
        python_callable=export_count_file,
        provide_context=True

    )

extract_order_from_northwind >> export_count_text >> export_final_output

