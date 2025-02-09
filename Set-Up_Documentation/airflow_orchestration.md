1. **Install Astro CLI**  
   - Use the command: `brew install astro`  

2. **Create a new Astro project**  
   - Run: `astro dev init`  

3. **Start the Astro environment locally**  
   - Run: `astro dev start`  

4. **Set up Docker Environment**  
   - Ensure Docker is installed and running  
   - The Astro project uses a `Dockerfile` with required dependencies like:
     - `dbt-snowflake`
     - `boto3`
     - `pandas`  
   - Necessary scripts (`mysql_s3_incremental_load.py`, `s3_snowflake_incremental_load.py`) and configuration files (`profiles.yml`, `dbt_project.yml`) must be copied into the container  

5. **Pipeline Workflow**  
   - Extract data from MySQL and load it into S3  
   - Transfer data from S3 into Snowflake  
   - Run dbt models for data transformation  
   - Test dbt models to validate transformations  

6. **Place the DAG file**  
   - Copy the DAG file into the `dags` folder inside the Astro project  

7. **Start Astro and Launch Airflow**  
   - Run: `astro dev start`  

8. **Log into the Airflow UI**  
   - Open the browser and go to `http://localhost:8080`  
   - Use the default login credentials:  
     - Username: `admin`  
     - Password: `admin`  

9. **Verify DAG Execution**  
   - The DAG should appear in the Airflow UI  
   - It can be triggered manually or will run automatically at `6am, 2pm, and 10pm SGT`  

10. **Validate Configurations**  
   - Run inside the Astro container: `dbt debug`  
