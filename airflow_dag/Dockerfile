FROM quay.io/astronomer/astro-runtime:12.6.0

# Install DBT and required Python packages globally
RUN pip install --no-cache-dir dbt-snowflake boto3 pandas apache-airflow-providers-snowflake

# Enable test_connection functionality in Airflow
ENV AIRFLOW__CORE__TEST_CONNECTION=enabled

# Set working directory for your scripts and DAGs
WORKDIR /home/astro

# Copy files into the container
COPY files /home/astro/files
COPY dags /home/astro/dags
COPY files/profiles.yml /home/astro/files/
COPY files/dbt_project.yml /home/astro/files/
