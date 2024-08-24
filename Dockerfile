FROM apache/airflow:2.9.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

RUN  python -m pip install --upgrade pip
RUN  python -m pip install python-dotenv

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID