FROM mysql:9.0

COPY ./mlflow/mlflow-mysql.cnf /etc/mysql/my.cnf
EXPOSE 3310
