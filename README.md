# Airflow Docker Setup Guide

This guide provides instructions on setting up Docker containers for running Apache Airflow and the DAGs defined in this repository.

### Initialization

To initialize your Docker containers, run the following commands. These commands will set up the necessary Docker containers for Airflow.

1. Initialize the Airflow environment:
    ```bash
    docker-compose up airflow-init
    ```

2. Build and start the containers in detached mode:
    ```bash
    docker-compose up --build -d
    ```

### Shutdown

To shut down the containers and remove the associated volumes, use the command:

    ```bash
    docker-compose down -v
    ```

## Accessing Airflow Webserver

To access the Airflow webserver's bash:

1. List all running Docker containers:
    ```
    docker ps
    ```

2. Identify the container ID of the Airflow webserver.

3. Access the bash of the webserver container using the following command (replace `<container_id>` with the actual container ID):
    ```
    docker exec -ti <container_id> bash
    ```

**Note:** When entering the `container_id`, it is not necessary to enter the entire ID; a part of it is usually sufficient.

## Necessary Airflow Variables

Set the following Airflow variables for proper configuration:

- `fingerprint_file_path`: Path to the fingerprint file, e.g., `/opt/airflow/config/fingerprint_file.yaml`
- `shodan_api_key`: Shodan API key (if using run_shodan_scan task)
- `shodan_search_phrase`: The search phrase for Shodan
- `divd_case_number`: The case number of the case that is being worked on

## SMTP Server Configuration

Adjust the SMTP server IP and credentials in your Airflow configuration:

```python
smtp_server = '192.168.43.145'  # TODO: Replace with actual SMTP server IP
smtp_port = 1025  # Default MailHog SMTP port, TODO: Replace with actual SMTP server port
smtp_username = 'hello@gmail.com'  # MailHog doesn't require username, TODO: Replace with real auth info
smtp_password = ''  # MailHog doesn't require password
```

## Note on Using Uncover for Scanning

If you are using `uncover` for scanning, don't forget to place your API keys in the `/config/uncover_api_keys.yaml` file. Structure the `.yaml` file as noted in the Uncover documentation. Be aware that the Uncover task may need further adjustments according to the specific use case (especially with parsing the output of uncover scan).



# Mailhog setup guide for testing

## MailHog Docker Setup Instructions
This document provides instructions for building and starting a MailHog Docker instance.

## Building and Starting MailHog

Navigate to the MailHog Directory: Make sure you are in the directory where the Dockerfile for MailHog is located.

```bash
cd path/to/mailhog/directory
```
Replace path/to/mailhog/directory with the actual path to your MailHog Dockerfile.

Build the Docker Image: Use the Docker build command to create an image for MailHog.

```bash
docker build -t mailhog-image .
```
This command builds a Docker image named mailhog-image from the Dockerfile in the current directory.

Run the MailHog Container: Start a container from the built image.

```bash
docker run -d -p 1025:1025 -p 8025:8025 --name mailhog mailhog-image
```
- -d runs the container in detached mode.
- -p 1025:1025 maps the SMTP port.
-p 8025:8025 maps the web interface port.
--name mailhog names the container mailhog.
After running this command, MailHog will be accessible on port 8025 for the web interface and port 1025 for SMTP.

Accessing MailHog
Web Interface: Open a browser and navigate to http://localhost:8025 to access the MailHog web interface.
SMTP Server: Use localhost with port 1025 as the SMTP server.
