FROM apache/airflow:2.10.0

COPY ./airflow/airflow_requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /airflow_requirements.txt
