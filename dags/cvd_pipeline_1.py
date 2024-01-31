from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import yaml
from datetime import datetime
import subprocess
from subprocess import CalledProcessError
import json
import requests
import os

def read_vulnerability_file(**kwargs):
    file_path = Variable.get("fingerprint_file_path")
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    # Pushing the data to XCom
    kwargs['ti'].xcom_push(key='yaml_vulnerability_data', value=data) 



# Currently not used
def run_uncover_scan(**kwargs):
    # Pulling the data from XCom
    ti = kwargs['ti']
    yaml_data = ti.xcom_pull(key='yaml_vulnerability_data', task_ids='read_vulnerability_file')
    
    # If getting the search phrase from the fingerprint file...
    # ...

    # Get uncover search phrase from Airflow Variable
    uncover_search_phrase = Variable.get("uncover_search_phrase")

    # Running uncover with the provided search phrase
    shodan_command = f"uncover -s '{uncover_search_phrase}' -l 9999999 > data/shodan-results.txt"
    censys_command = f"uncover -cs '{uncover_search_phrase}' -l 9999999 > censys-results.txt"

    # Execute the commands and handle errors
    try:
        subprocess.run(shodan_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while running Shodan command: {e}")

    try:
        subprocess.run(censys_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while running Censys command: {e}")
    
    print("Uncover scan completed.")



def run_shodan_scan(**kwargs):
    ti = kwargs['ti']
    # Get Shodan search phrase and date from Airflow Variable
    shodan_search_phrase = Variable.get("shodan_search_phrase")
    shodan_api_key = Variable.get("shodan_api_key")

    current_date = datetime.now().strftime("%Y-%m-%d")

    # Constructing the Shodan download command
    #download_command = f"shodan download --limit -1 'data/{current_date}-download' '{shodan_search_phrase}'"
    download_command = f"shodan download --limit 5 /opt/airflow/data/{current_date}-download '{shodan_search_phrase}'"
    print(download_command)

    # Initialize Shodan with the API key - needs to be set in Airflow UI
    init_command = f"shodan init {shodan_api_key}"
    try:
        subprocess.run(init_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while initializing Shodan: {e}")
        return

    # Execute the download command and handle errors
    try:
        subprocess.run(download_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while running Shodan download command: {e}")

    # Constructing the Shodan convert command to get ip_str and port in CSV format
    convert_command = f"shodan parse --fields ip_str,port --separator : 'data/{current_date}-download.json.gz' > 'data/{current_date}-shodan-results.csv'"

    # Execute the convert command and handle errors
    try:
        subprocess.run(convert_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while converting Shodan data: {e}")

    print("Shodan scan and conversion completed.")



def run_nuclei_scan(**kwargs):
    ti = kwargs['ti']
    current_date = datetime.now().strftime("%Y-%m-%d")
    shodan_results_path = f"data/{current_date}-shodan-results.csv"  # Path to the CSV file generated by the previous task

    fingerprint_file_path = Variable.get("fingerprint_file_path")
    
    # Nuclei command with parameters used by DIVD
    nuclei_command = (
        f"nuclei -H 'User-Agent: DIVD-2023-00020' -t {fingerprint_file_path} "
        f"-l {shodan_results_path} -retries 2 -timeout 6 "
        f"-output data/{current_date}-nuclei-results.json -j"
    )

    # Execute the Nuclei command and handle errors
    try:
        subprocess.run(nuclei_command, shell=True, check=True)
    except CalledProcessError as e:
        print(f"An error occurred while running Nuclei command: {e}")
        print(e.output)
    

    # File path for the Nuclei JSON output
    nuclei_output_path = f"data/{current_date}-nuclei-results.json"

    try:
        with open(nuclei_output_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Process each JSON object separately
        for i, line in enumerate(lines):
            try:
                data = json.loads(line)
                with open(f"data/nuclei_json_results/formatted_output_{i}.json", 'w', encoding='utf-8') as outfile:
                    json.dump(data, outfile, ensure_ascii=False, indent=4)
                print(f"JSON object {i} has been formatted and saved")
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON object {i}: {e}")

    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
    
    print("Nuclei scan completed.")



def enrich_nuclei_data(**kwargs):
    ti = kwargs['ti']
    nuclei_results_directory = 'data/nuclei_json_results'  # Directory containing JSON files
    enriched_directory = 'data/enriched_nuclei_results'
    no_contact_info_directory = 'data/no_contact_info_nuclei_results'

    # Ensure directories exist
    os.makedirs(enriched_directory, exist_ok=True)
    os.makedirs(no_contact_info_directory, exist_ok=True)

    # Iterate over each file in the directory
    for filename in os.listdir(nuclei_results_directory):
        if filename.endswith('.json'):  # Ensures processing only JSON files
            file_path = os.path.join(nuclei_results_directory, filename)

            try:
                # Read the Nuclei results
                with open(file_path, 'r') as file:
                    entry = json.load(file)  # Directly load the JSON object

                # Debugging: Check the type of the loaded object
                print(f"Type of loaded object: {type(entry)}, Content: {entry}")

                if not isinstance(entry, dict):
                    print(f"Unexpected data structure in {filename}: {entry}")
                    continue  # Skip to the next file

                contact_info_found = False  # Flag to track if contact info is found
                ip_or_domain = entry.get('ip') or entry.get('host')  # Adjust based on your Nuclei results structure

                # If IP or domain is present, gather additional info
                if ip_or_domain:
                    # Gather contact info from RIPEstat
                    try:
                        ripestat_data = requests.get(f'https://stat.ripe.net/data/whois/data.json?resource={ip_or_domain}').json()
                        entry['ripestat_contact_info'] = ripestat_data  # Adjust if necessary
                        print(ripestat_data)
                        contact_info_found = True
                    except requests.exceptions.RequestException as e:
                        print(f"An error occurred while fetching data from RIPEstat for {ip_or_domain}: {e}")
                        entry['ripestat_contact_info'] = 'Error fetching data'

                    # Gather contact info from Whois
                    try:
                        whois_data = subprocess.getoutput(f'whois {ip_or_domain}')
                        entry['whois_contact_info'] = whois_data
                        print(whois_data)
                        contact_info_found = True
                    except Exception as e:
                        print(f"An error occurred while fetching data from Whois for {ip_or_domain}: {e}")
                        entry['whois_contact_info'] = 'Error fetching data'

                # Save or handle the enriched data
                if contact_info_found:
                    save_directory = enriched_directory
                else:
                    save_directory = no_contact_info_directory

                output_file_path = os.path.join(save_directory, f'enriched_{filename}')
                with open(output_file_path, 'w') as file:
                    json.dump(entry, file)  # Dump the single entry

                print(f"Nuclei data enrichment completed for {filename}.")

            except Exception as e:
                print(f"An error occurred while processing the file {filename}: {e}")

    print("Enrich step completed.")




default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}



with DAG(
    dag_id='cvd_automation_pipeline', 
    default_args=default_args, 
    schedule_interval=None,  # set to None for manual trigger
    description='A DAG for vulnerability scanning',
    catchup=False  # set to False if you don't want backfilling
) as dag:
    #read_vulnerability_file_task = PythonOperator(
    #    task_id='read_vulnerability_file_task',
    #    python_callable=read_vulnerability_file,
    #    provide_context=True
    #)

    run_shodan_scan_task = PythonOperator(
        task_id='run_shodan_scan_task',
        python_callable=run_shodan_scan,
        provide_context=True
    )

    run_nuclei_scan_task = PythonOperator(
        task_id='run_nuclei_scan_task',
        python_callable=run_nuclei_scan,
        provide_context=True
    )

    enrich_nuclei_data_task = PythonOperator(
        task_id='enrich_nuclei_data_task',
        python_callable=enrich_nuclei_data,
        provide_context=True
    )

    #read_vulnerability_file_task >> run_shodan_scan_task >> run_nuclei_scan_task >> enrich_nuclei_data_task
    run_shodan_scan_task >> run_nuclei_scan_task >> enrich_nuclei_data_task
