# Dockerfile
FROM jeet/airflow-spark-base:latest

USER airflow
WORKDIR /opt/airflow

# copy only lightweight files
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# optional: custom Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]
