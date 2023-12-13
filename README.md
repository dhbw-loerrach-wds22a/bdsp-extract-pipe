
# Updated README for Running Airflow DAGs

## Prerequisites:
- Docker and Docker Compose installed on your machine.
- Clone the repository containing the Docker Compose file and the related folders.

## Starting the Environment:
### Start the Services:
- Navigate to the directory containing the Docker Compose file.
- Run `docker-compose up -d` to start all the services in detached mode.

### Verify Services:
- Ensure that the MySQL, Spark, and Airflow services are running correctly.
- Access phpMyAdmin at `http://localhost:8082` to check the MySQL database.
- Spark Master UI available at `http://localhost:8080`.
- Airflow Webserver accessible at `http://localhost:8081`.

## Adding and Running the DAGs:
### Prepare the DAG Files:
- Place the `live_data_pipeline.py` and `historic_pipeline.py` files in the `../bdsp-extract-pipe/dags` directory. This directory is mounted into the Airflow container.

### Access Airflow Web Interface:
- Open `http://localhost:8081` in a web browser to access the Airflow UI.

### Trigger the DAGs:
- In the Airflow UI, locate the `live_data_pipeline` and `historic_data_pipeline` DAGs.
- Manually trigger each DAG or set up a schedule for automatic execution.

### Monitoring and Logs:
- Use the Airflow web interface to monitor the status of your DAGs.
- Check the logs in the Airflow UI for each task to debug if needed.

## Stopping the Environment:
- To stop and remove the containers, networks, and volumes created by Docker Compose, run `docker-compose down`.

## Notes:
- Ensure that the Python scripts referenced in the DAGs are present in the correct paths as specified in the Docker Compose volume mounts.
- Update environment variables and ports in the Docker Compose file as needed based on your specific requirements.
