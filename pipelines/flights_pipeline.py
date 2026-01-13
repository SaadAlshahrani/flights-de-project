from ingestion.fetch_flights import run_ingestion
from transformations.transform_flights import run_transformation
from prefect import task, flow


@task(
        retries=3,
        retry_delay_seconds=30,
        name="flights ingestion task"
)
def ingestion_task():
    return run_ingestion()


@task(
        retries=2,
        retry_delay_seconds=30,
        name="flights transformation task"
)
def transformation_task(raw_data_path):
    run_transformation(raw_data_path)

@flow(name="flights pipeline")
def flights_pipeline():
    raw_data_path = ingestion_task()
    transformation_task(raw_data_path)


if __name__ == "__main__":
    flights_pipeline()