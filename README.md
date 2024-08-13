# datafam_birthday

This project includes:
* Data pipeline on Apache Airflow on Docker
* Dashboard on Tableau

The collection of the datafam birthday and my interest in calender viz spike this interest. The data is collected via a google form and stored in a sheet. In order to automatically update the calender dashboard as new entries comes in, I decided to adopt the data engineer role to extract the data from its source(google sheet), load into a database(postgreSQL) and final result displayed in Tableau.

follow doc to set up docker https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

gspread doc https://docs.gspread.org/en/v6.0.0/user-guide.html

https://docs.gspread.org/en/latest/oauth2.html

