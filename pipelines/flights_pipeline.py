from ingestion.fetch_flights import run_ingestion
from transformations.transform_flights import run_transformation


def main():
    raw_data_path = run_ingestion()
    run_transformation(raw_data_path)


if __name__ == "__main__":
    main()