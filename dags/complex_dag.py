from airflow.decorators import dag, task
from pendulum import datetime

import duckdb
import pandas as pd

CSV_PATH = "ducks.csv"
LOCAL_DUCKDB_STORAGE_PATH = "my_local_ducks.db"


@dag(
        start_date=datetime(2024, 5, 15)
        , schedule='@daily'
        , catchup=False
        ,params={"color_to_store": "blue"}
    )
def complex_dag():
    @task()
    def create_table_in_memory_db() -> int:
        print("Creating a temporary in-memory DuckDB database.")

        conn = duckdb.connect()
        conn.sql(
            f"""CREATE TABLE IF NOT EXISTS in_memory_duck_table AS 
            SELECT * FROM read_csv_auto('{CSV_PATH}', header=True);"""
        )
        duck_species_count = conn.sql(
            "SELECT count(*) FROM in_memory_duck_table;"
        ).fetchone()[0]
        return duck_species_count

    @task()
    def create_pandas_df() -> pd.DataFrame:
        "Create a pandas DataFrame with toy data and return it."
        ducks_in_my_garden_df = pd.DataFrame(
            {"colors": ["blue", "red", "yellow"], "numbers": [2, 3, 4]}
        )

        return ducks_in_my_garden_df

    @task()
    def create_table_from_pandas_df(ducks_in_my_garden_df: pd.DataFrame, local_duckdb_storage_path: str) -> None:
        "Create a table based on a pandas DataFrame."

        conn = duckdb.connect(local_duckdb_storage_path)
        conn.sql(
            f"""CREATE TABLE IF NOT EXISTS ducks_garden AS 
            SELECT * FROM ducks_in_my_garden_df;"""
        )

    @task()
    def create_table_in_local_persistent_storage(local_duckdb_storage_path: str) -> int:
        "Create a table in a local persistent DuckDB database."

        conn = duckdb.connect(local_duckdb_storage_path)
        conn.sql(
            f"""CREATE TABLE IF NOT EXISTS persistent_duck_table AS 
            SELECT * FROM read_csv_auto('{CSV_PATH}', header=True);"""
        )
        duck_species_count = conn.sql(
            "SELECT count(*) FROM persistent_duck_table;"
        ).fetchone()[0]
        return duck_species_count

    @task(task_id='blue_filter')
    def query_persistent_local_storage_blue(local_duckdb_storage_path: str) -> list:
        "Query a table in a local persistent DuckDB database."

        conn = duckdb.connect(local_duckdb_storage_path)
        species_with_blue_in_name = conn.sql(
            """SELECT species_name FROM persistent_duck_table 
            WHERE species_name LIKE '%Blue%';"""
        ).fetchall()

        blue_df = pd.DataFrame(species_with_blue_in_name)

        conn.sql(
            """CREATE TABLE IF NOT EXISTS blue_ducks AS 
            SELECT * FROM blue_df;"""
        )
        return species_with_blue_in_name
    
    @task(task_id='green_filter')
    def query_persistent_local_storage_green(local_duckdb_storage_path: str) -> list:
        "Query a table in a local persistent DuckDB database."

        conn = duckdb.connect(local_duckdb_storage_path)
        species_with_green_in_name = conn.sql(
            """SELECT species_name FROM persistent_duck_table 
            WHERE species_name LIKE '%Green%';"""
        ).fetchall()

        green_df = pd.DataFrame(species_with_green_in_name)

        conn.sql(
            """CREATE TABLE IF NOT EXISTS green_ducks AS 
            SELECT * FROM green_df;"""
        )
        return species_with_green_in_name
    
    @task.branch()
    def choose_what_to_do(**context) -> str:
        if context["params"]["color_to_store"] == "blue":
            return "blue_filter"
        elif context["params"]["color_to_store"] == "green":
            return "green_filter"

    @task()
    def print_count(duck_species_count: int) -> None:
        print(f"Duck species count: {duck_species_count}")

    print_count(create_table_in_memory_db()) >> [create_table_from_pandas_df(create_pandas_df(), local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH)]

    create_table_in_local_persistent_storage(local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH) >> choose_what_to_do() >> [
        query_persistent_local_storage_blue(
            local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH
        ),
        query_persistent_local_storage_green(
            local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH
        )
    ]

complex_dag()