from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

default_args = {
    'owner': 'your_username',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'mailmerge_dag',
    default_args=default_args,
    description='A DAG to run Mailmerge manually',
    schedule_interval=None,  # Set to None to trigger manually
)

# Define input variables
email = Variable.get("email", default_var="example@email.com")
case_title = Variable.get("case_title", default_var="Default Case Title")
case_number = Variable.get("case_number", default_var="123")
host = Variable.get("host", default_var="example-host")
vulnerable_product = Variable.get("vulnerable_product", default_var="Example Product")
timestamp = Variable.get("timestamp", default_var="2024-01-15T12:00:00")

# Mailmerge command with substituted variables
mailmerge_command = (
    f"cd /opt/airflow/data && mailmerge --no-dry-run --no-limit --template /opt/airflow/data/mailmerge_template.txt"
)

mailmerge_task = BashOperator(
    task_id='run_mailmerge',
    bash_command=mailmerge_command,
    dag=dag,
)

mailmerge_task

