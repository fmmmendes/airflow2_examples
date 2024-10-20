# Airflow 2 Examples

[![Lint Code Base](https://github.com/fmmmendes/airflow2_examples/actions/workflows/linter.yml/badge.svg?branch=main)](https://github.com/fmmmendes/airflow2_examples/actions/workflows/linter.yml)
## Setup

The setup is based on airflow2 documentation available on this [link](https://airflow.apache.org/docs/apache-airflow/2.3.2/start/docker.html) with more details.

The following commands are shortcuts docker deploy. The docker-compose file is not the original, it has an update related to the airflow config.


```bash
docker compose up airflow-init

docker compose up

```

When the services are all up, check the followting link: localhost:8080

Use the user and password: "airflow"

To stop the containers use this command

```bash
docker compose stop

```

To stop and remove the container use this command

```bash
docker compose down
# use this on instead to remove volumes
docker compose down --volumes --rmi all
```

## Config

Any changes on config file, it will require a restart of the webserver container

## Import Variables

TODO

## Clean Logs Folder

TODO
## References

https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html   
https://pendulum.eustace.io/docs/#introduction   
https://airflow.apache.org/docs/apache-airflow/stable/howto/customize-ui.html   

