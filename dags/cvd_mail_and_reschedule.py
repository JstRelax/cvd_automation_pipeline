from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import json
import os
import re
import csv
import logging
import requests
import smtplib
import subprocess
from subprocess import CalledProcessError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template


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
    smtp_port = 1025  # Default MailHog SMTP port
    smtp_username = 'hello@gmail.com'  # MailHog doesn't require username
    smtp_password = ''  # MailHog doesn't require password
    logging.info("SMTP server connecting...")

    # Establish SMTP connection
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        logging.info("SMTP server connected successfully.")
    except Exception as e:
        logging.error(f"Failed to connect to SMTP server: {e}")
        return

    divd_case_number = Variable.get("divd_case_number")
    
    # Check if rescan file exists, else use initial file
    rescan_file_path = f'/opt/airflow/data/{divd_case_number}/enriched_results_rescan.json'
    initial_file_path = f'/opt/airflow/data/{divd_case_number}/enriched_results_with_email.json'
    json_file = rescan_file_path if os.path.exists(rescan_file_path) else initial_file_path

    # Load JSON data
    try:
        with open(json_file, 'r') as file:
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

            # Create and send message
            msg = MIMEMultipart()
            msg['From'] = smtp_username
            msg['To'] = email
            msg['Subject'] = "Vulnerability Notification - " + ip
            msg.attach(MIMEText(email_body, 'plain'))
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

def convert_json_to_csv(json_file_path, csv_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        json_data = json.load(json_file)

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in json_data.items():
            host = value.get("host", "")
            if host:
                writer.writerow([host])

def run_nuclei_scan(**kwargs):
    ti = kwargs['ti']
    current_date = datetime.now().strftime("%Y-%m-%d")
    fingerprint_file_path = Variable.get("fingerprint_file_path")
    divd_case_number = Variable.get("divd_case_number")

    # JSON file path
    json_file_path = f"data/{divd_case_number}/enriched_results.json"

    # New CSV file path
    contacted_targets = f"data/{divd_case_number}/contacted_targets.csv"

    # Convert JSON to CSV
    convert_json_to_csv(json_file_path, contacted_targets)
    
    # Nuclei command with parameters used by DIVD
    nuclei_command = (
        f"nuclei -H 'User-Agent: {divd_case_number}' -t {fingerprint_file_path} "
        f"-l {contacted_targets} -retries 2 -timeout 6 "
        f"-output data/{divd_case_number}/nuclei_results_rescan.json -j"
    )

    # Execute the Nuclei command and handle errors
    try:
        subprocess.run(nuclei_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while running Nuclei command: {e}")
    
    print("Nuclei scan completed.")

def enrich_nuclei_data(**kwargs):
    current_date = datetime.now().strftime("%Y-%m-%d")
    divd_case_number = Variable.get("divd_case_number")

    # Adjusting file paths to use divd_case_number folder
    nuclei_output_path = f"data/{divd_case_number}/nuclei_results_rescan.json"
    enriched_results_path = f'data/{divd_case_number}/enriched_results_rescan.json'

    # Ensure the enriched directory exists
    #os.makedirs(enriched_directory, exist_ok=True)

    # Run nuclei-parse-enrich tool and save output to a file
    cmd = f'cd /opt/airflow/nuclei-parse-enrich && go run cmd/main.go -i ../{nuclei_output_path} -o ../{enriched_results_path}'
    subprocess.run(cmd, shell=True, check=True)

    # Load the output of the nuclei-parse-enrich tool
    #enriched_output_path = os.path.join(enriched_directory, f'enriched_{current_date}.json')
    with open(enriched_results_path, 'r') as file:
        enriched_data = json.load(file)

    # Iterate over each entry and enrich with security.txt data
    for ip, entry in enriched_data.items():
        ip_or_domain = entry.get('ip') or entry.get('host')
        if ip_or_domain:
            try:
                response = requests.get(f'https://{ip_or_domain}/.well-known/security.txt')
                if response.status_code == 200:
                    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', response.text)
                    entry['security_txt_email'] = emails[0] if emails else None
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving security.txt for {ip_or_domain}: {e}")

    # Save the enriched data
    #output_file_path = os.path.join(enriched_directory, f'enriched_results.json')
    with open(enriched_results_path, 'w') as file:
        json.dump(enriched_data, file, indent=4)  # Add indentation for formatting

    print("Enrichment step completed.")

def record_start_time(**kwargs):
    start_time = datetime.now()
    Variable.set("dag_start_time", start_time.isoformat())

def check_if_8_days_passed(**kwargs):
    start_time_str = Variable.get("dag_start_time", default_var=None)
    if start_time_str:
        start_time = datetime.fromisoformat(start_time_str)
        return datetime.now() >= start_time + timedelta(days=8)
    return False

def check_time_condition(**kwargs):
    start_time_str = Variable.get("dag_start_time", default_var=None)
    if start_time_str:
        start_time = datetime.fromisoformat(start_time_str)
        return datetime.now() >= start_time + timedelta(seconds=120)  # For testing, set to 30 seconds
    return False

def log_message(**kwargs):
    logging.info("Test log message LOOOOOOOOOOOOOOOOOOOOOOOOOOGGGGGG")


with DAG('vulnerability_email_notification',
         default_args=default_args,
         description='Send email notifications for vulnerabilities',
         schedule_interval=None,  # Set to None for manual trigger
         start_date=datetime(2024, 1, 1),
         catchup=False) as dag:

    record_start_date_task = PythonOperator(
        task_id='record_start_date',
        python_callable=record_start_time,
    )

    email_vulnerability_information = PythonOperator(
        task_id='email_vulnerability_information',
        python_callable=process_vulnerability_data
    )

    #wait_8_days = PythonSensor(
    #    task_id='wait_8_days',
    #    python_callable=check_if_8_days_passed,
    #    mode='reschedule',
    #    poke_interval=60 * 60 * 24, # Check daily
    #    timeout=60 * 60 * 24 * 10, # Must be over 8 days... set to 10 just in case
    #)

    wait_for_some_time = PythonSensor(
        task_id='wait_for_some_time',
        python_callable=check_time_condition,
        mode='reschedule',
        poke_interval=10, # Check every 10 seconds
        timeout=200, # Timeout after 200 seconds
    )

    task_conditional_scan = PythonOperator(
        task_id='conditional_nuclei_scan',
        python_callable=run_nuclei_scan
    )

    #log_message_task = PythonOperator(
    #    task_id='log_message_task',
    #    python_callable=log_message
    #)

    enrich_nuclei_data_task = PythonOperator(
        task_id='enrich_nuclei_data_task',
        python_callable=enrich_nuclei_data,
        provide_context=True
    )

    email_second_time = PythonOperator(
        task_id='email_vulnerability_information_second_time',
        python_callable=process_vulnerability_data
    )

    record_start_date_task >> email_vulnerability_information >> wait_for_some_time >> task_conditional_scan >> enrich_nuclei_data_task >> email_second_time