FROM apache/airflow:2.10.5-python3.8

ENV AIRFLOW_HOME=/opt/airflow

USER airflow

RUN pip install "uv>=0.1.33"

COPY requirements.txt /requirements.txt
RUN uv pip install --no-cache -r /requirements.txt