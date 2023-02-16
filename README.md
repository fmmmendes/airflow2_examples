# Airflow 2

## Setup


https://airflow.apache.org/docs/apache-airflow/2.3.2/start/docker.html


curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.2/docker-compose.yaml'

docker-compose up airflow-init

docker-compose up

localhost:8080

docker-compose stop

docker-compose down

docker-compose down --volumes --rmi all

## Condig

Any changes on config file, it will require a restart of the webserver container

## References

https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
https://pendulum.eustace.io/docs/#introduction
https://airflow.apache.org/docs/apache-airflow/stable/howto/customize-ui.html

