from prefect import flow, task
import os

@task(name="Ingest New Data")
def run_ingestion():
    os.system("python ingestion/load.py")

@task(name="Transform with dbt")
def run_dbt():
    os.chdir("kalshi_dbt")
    os.system("dbt run")
    os.chdir("..")

@flow(name="Kalshi Batch Analytics Pipeline", log_prints=True)
def daily_pipeline():
    print("Starting Kalshi pipeline...")
    run_ingestion()
    run_dbt()
    print("Pipeline complete!")

if __name__ == "__main__":
    daily_pipeline()