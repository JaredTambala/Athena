FROM mysql:latest

COPY ./mlflow/mlflow-mysql.cnf /etc/mysql/my.cnf
EXPOSE 3310
