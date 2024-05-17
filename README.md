# Airflow Standalone deploy

Repo where you can find a stand alone deploy of Airflow, with three example DAGs.

# Steps to repoduce the deploy
1. Go to the airflow-test folder
2. Run the docker compose command to raise the services:
````docker-compose up -d````
3. Once the three services are running and up, we need to load the file to play with the examples:
   1. search for the scheduler and webserver container IDs: ````docker ps````
   2. copy the file to those containers: ````docker cp include/ducks.csv {CONTAINER ID}:/opt/airflow````
4. Open your browser and enter ````localhost:8080```` to acces to the UI
5. Enter the credentials: ````user:admin```` and ````password: admin````

That'st all, enjoy Airflow.