## Datafam Birthday

### Objective
Build an ETL pipeline that process data in batch: periodically(daily)

* Using the DataFam birthday data collected from the community.
* Create a pipeline to put data in a database
* Transform the data 
* Build a calender dashboard to visualize the data

### Technologies
* Data Ingestion and Transformation : Python
* Workflow Orchestration : Apache Airflow
* Database : OLTP(PostgreSQL)
* Visualization : Tableau

### Reference
* Follow doc to set up docker https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

* Gspread doc https://docs.gspread.org/en/v6.0.0/user-guide.html, https://docs.gspread.org/en/latest/oauth2.html

Airflow: port 8282

Pgadmin: port 8001

Postgres db host on Airflow: host.docker.internal