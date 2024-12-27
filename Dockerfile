FROM quay.io/astronomer/astro-runtime:12.2.0

# Switch to root to install Terraform
USER root

# Install required system packages and Terraform
RUN apt-get update && \
    apt-get install -y wget unzip && \
    wget https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip && \
    unzip terraform_1.9.8_linux_amd64.zip -d /usr/local/bin && \
    rm terraform_1.9.8_linux_amd64.zip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the default 'astro' user    
USER astro

# Create and activate Python virtual environment, install dbt-bigquery
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && \
    deactivate
