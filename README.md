# BDSP Extract Pipeline

This document provides instructions on how to run Python scripts in the Dockerized environment of the BDSP Extract Pipeline.

## Getting Started

These instructions will get you through the process of running a Python script inside a Docker container.

### Prerequisites

Before you begin, make sure you have the following installed:
- Docker
- Docker Compose

### Running the Script

To execute the `extract.py` script inside the Docker container, follow these steps:

The docker-compose file is inside the `bdsp-services` repo.
1. Start your Docker containers using Docker Compose:
   
   ```bash
   docker-compose up -d

2. Once the containers are running, you can execute the `extract.py` script inside the `spark-worker` container with the following command:

    ```bash
    docker exec -it services_spark-worker_1 python extract.py
