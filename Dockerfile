# Start from the official Airflow image
FROM apache/airflow:2.8.1

# User root is required to install packages
USER root

# Update the package repository
RUN apt-get update

# Install wget
RUN apt-get install -y --no-install-recommends wget

# Install unzip
RUN apt-get install -y --no-install-recommends unzip

# Install Git
RUN apt-get install -y --no-install-recommends git

# Install python3-pip
RUN apt-get install -y --no-install-recommends python3-pip

# Install whois
RUN apt-get install -y --no-install-recommends whois

# Clean up the package manager
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Go
RUN wget https://dl.google.com/go/go1.18.1.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.18.1.linux-amd64.tar.gz && \
    rm go1.18.1.linux-amd64.tar.gz

# Set PATH to include Go binaries
ENV PATH="${PATH}:/usr/local/go/bin"
# Create and set permissions for the Go cache directory
RUN mkdir -p /opt/airflow/go-cache && chown -R airflow /opt/airflow/go-cache && chmod 777 /opt/airflow/go-cache
# Set Go cache directory to a location with write permission
ENV GOCACHE=/opt/airflow/go-cache

# Download and install nuclei
RUN wget https://github.com/projectdiscovery/nuclei/releases/download/v3.1.7/nuclei_3.1.7_linux_amd64.zip && \
    unzip -o nuclei_3.1.7_linux_amd64.zip && \
    mv nuclei /usr/local/bin/ && \
    rm nuclei_3.1.7_linux_amd64.zip

# Download and install uncover
RUN wget https://github.com/projectdiscovery/uncover/releases/download/v1.0.7/uncover_1.0.7_linux_amd64.zip && \
    unzip -o uncover_1.0.7_linux_amd64.zip && \
    mv uncover /usr/local/bin/ && \
    rm uncover_1.0.7_linux_amd64.zip

# Clone nuclei-parse-enrich repository
RUN git clone https://github.com/DIVD-NL/nuclei-parse-enrich.git && \
    chown -R airflow nuclei-parse-enrich

# Switch back to the airflow user
USER airflow

# Install shodan using pip as the airflow user
RUN pip install --user shodan

# Install mailmerge
RUN pip install mailmerge