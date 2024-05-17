#!/usr/bin/env bash

# Initialize external database
airflow db init
# Create default user to access the UI
airflow users create --username admin --password admin --firstname First --lastname Last --role Admin --email admin@example.com
# Start the webserver
airflow webserver