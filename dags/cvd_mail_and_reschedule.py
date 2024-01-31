from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from jinja2 import Template
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


# Template string
template_string = '''TO: {{ email }}
SUBJECT: Case Title - DIVD Case Number - {{ host }}
FROM: DIVD-CSIRT <{{ divd_case_number }}@csirt.divd.nl>
REPLY-TO: DIVD-CSIRT <csirt@divd.nl>

Hi,

Researchers of DIVD have identified vulnerabilities in your network. These vulnerabilities are critical and require your immediate attention.

Scan data:
- Scan time: {{ timestamp }}
- Host: {{ host }}

We determined the vulnerability using specific security tests. No changes or harm to your system has been made.

DIVD case file: https://csirt.divd.nl/cases/{{ divd_case_number }}
Vendor Security Advisory: [Security Advisory URL]

If you have any questions or need help in mitigating this vulnerability, please contact us at csirt@divd.nl.

DIVD-CSIRT is part of DIVD, a non-profit organization that strives to make the Internet safer. More information about DIVD can be found at https://divd.nl.

Thank you for your time and attention.

DIVD-CSIRT

P.S. If you are not running this server yourself but know the responsible party (e.g., ISP or hosting party), please forward this information to them. You have our explicit approval for this.'''

# Create a Jinja2 template
template = Template(template_string)

divd_case_number = "DIVD-2024-XXXX"

def process_vulnerability_data(**kwargs):
    smtp_server = '192.168.1.74'  # Host private IP address
    smtp_port = 1025            # Default MailHog SMTP port
    smtp_username = 'hello@gmail.com'          # MailHog doesn't require username - still here for template
    smtp_password = ''          # MailHog doesn't require password
    logging.info("SMTP server connecting...")
    # Establish SMTP connection
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        # Additional SMTP setup if needed
        # server.starttls()
        # server.login(smtp_username, smtp_password)
        logging.info("SMTP server connected successfully.")
    except Exception as e:
        logging.error(f"Failed to connect to SMTP server: {e}")
        return

    # Load JSON data
    try:
        with open('/opt/airflow/data/enriched_nuclei_results/enriched_2024-01-30.json', 'r') as file:
            data = json.load(file)
        logging.info("JSON data loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading JSON data: {e}")
        return

    for ip, details in data.items():
        abuse_emails = details['Abuse'].split(';')
        for email in abuse_emails:
            email_body = template.render(
                email=email,
                host=ip,
                timestamp=details['timestamp'],
                divd_case_number=divd_case_number,
            )

            # Create message
            msg = MIMEMultipart()
            msg['From'] = smtp_username
            msg['To'] = email
            msg['Subject'] = "Vulnerability Notification - " + ip
            msg.attach(MIMEText(email_body, 'plain'))

            # Send email
            try:
                server.send_message(msg)
                logging.info(f"Email sent to {email}")
            except Exception as e:
                logging.error(f"Error sending email to {email}: {e}")

    # Close SMTP connection
    try:
        server.quit()
        logging.info("SMTP server connection closed successfully.")
    except Exception as e:
        logging.error(f"Error closing SMTP server connection: {e}")

with DAG('vulnerability_email_notification',
         default_args=default_args,
         description='Send email notifications for vulnerabilities',
         schedule_interval=None,  # Set to None for manual trigger
         start_date=datetime(2024, 1, 1),
         catchup=False) as dag:

    task_process_data = PythonOperator(
        task_id='process_vulnerability_data',
        python_callable=process_vulnerability_data
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_next_dag",
        trigger_dag_id="second_dag_id",  # rescan and notify dag
        wait_for_completion=False,
        poke_interval=180,       # Checks condition every 30 seconds
        timeout=14400           # 4 hours timeout in seconds
    )

    task_process_data >> trigger_next_dag