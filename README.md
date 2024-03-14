# MinMaxAvgTempInSpark
This repository gives a basic idea of how to get started with spark with a small example of min,max, avg of temperature in spark



# Spark Docker Compose Setup

This Docker Compose configuration allows you to easily deploy an Apache Spark cluster locally using Docker containers.

## Prerequisites

- Docker installed on your machine

## Getting Started


1. Navigate to the cloned directory:

   ```bash
   cd spark-docker-compose
   ```

2. Start the Spark cluster:

   ```bash
   docker-compose up -d
   ```

   This command will start the Spark Master, Worker nodes, and History Server in detached mode.

3. To stop the cluster:

   ```bash
   docker-compose down
   ```

## Accessing the UI

- **Spark Master UI:** Visit [http://localhost:8080](http://localhost:8080) in your web browser to access the Spark Master UI.
- **Spark History Server UI:** Visit [http://localhost:18081](http://localhost:18081) to access the Spark History Server UI after jobs have been run.

## Configuration

- By default, this Docker Compose setup uses the `bde2020/spark-master:3.3.0-hadoop3.3` and `bde2020/spark-worker:3.3.0-hadoop3.3` images for Spark Master and Worker nodes respectively.
- You can customize the configuration by modifying the `docker-compose.yml` file.