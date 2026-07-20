## Overview

This guide helps you install Airflow using Docker.

## 1. Create network & Build docker image
**Create network**

```shell
docker network create streaming-network --driver bridge
```

**Build custom docker image**

```
docker build -t unigap/airflow:2.10.4 .
```

## 2. Initializing Environment

### 2.1 Setting the right Airflow user

Create the following directories: `dags`, `logs`, `plugins`, `config`

```shell
mkdir -p ./dags ./logs ./plugins ./config
```

Retrieve the user ID using the following command:

```shell
id -u
```

And the group ID of the `docker` group using:

```shell
getent group docker
```

Set the retrieved values into the `AIRFLOW_UID` and `DOCKER_GID` variables in the `.env` file.

### 2.2 Initialize airflow.cfg

```shell
docker compose run airflow-cli bash -c "airflow config list > /opt/airflow/config/airflow.cfg"
```

### 2.3 Initialize the database

```shell
docker compose up airflow-init
```

## 3. Running Airflow

```shell
docker compose up -d
```

This command will start the following Docker containers:

airflow-scheduler - The scheduler monitors all tasks and dags, then triggers the task instances once their dependencies
are complete.

airflow-dag-processor - The DAG processor parses DAG files.

airflow-api-server - The api server is available at http://localhost:18080.

airflow-worker - The worker that executes the tasks given by the scheduler.

airflow-triggerer - The triggerer runs an event loop for deferrable tasks.

airflow-init - The initialization service.

postgres - The database.

redis - The redis - broker that forwards messages from scheduler to worker.

**Check Status**

```shell
docker compose ps
```

## 4. Accessing the web interface

[web interface](http://localhost:18080)

username/password: `airflow/airflow`

## References

[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)