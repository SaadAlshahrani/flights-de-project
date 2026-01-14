import json
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    filename="transform_flights.log",
    filemode="a",
    encoding="utf-8",
    style="{",
    format="{asctime} - {levelname} - {message}",
    datefmt="%Y-%m-%d %H:%M",
)


def load_raw_flights_data(raw_payload_path):
    path_obj = Path(raw_payload_path)
    filename = path_obj.stem
    parent_directory = path_obj.parent.name

    try:
        with open(raw_payload_path, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)
        return raw_payload, filename, parent_directory

    except FileNotFoundError:
        logging.error(f"File not found.")
        raise

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from file.")
        raise

    except Exception as e:
        logging.error(f"An unexpected error occured. {e}")
        raise


def transform_raw_flight_data(raw_payload):
    ingested_at = raw_payload.get("metadata", {}).get("ingested_at", {})
    raw_data = raw_payload.get("data", {})
    flattened_data = pd.json_normalize(raw_data["data"])

    required_columns = [
        "flight_date",
        "flight_status",
        # Departure
        "departure.airport",
        "departure.iata",
        "departure.terminal",
        "departure.delay",
        "departure.scheduled",
        "departure.estimated",
        "departure.actual",
        # Arrival
        "arrival.airport",
        "arrival.iata",
        "arrival.terminal",
        "arrival.delay",
        "arrival.scheduled",
        "arrival.estimated",
        "arrival.actual",
        # Airline
        "airline.name",
        "airline.iata",
        # Flight
        "flight.number",
        "flight.iata",
        # Codeshared Flights
        "flight.codeshared.airline_name",
        "flight.codeshared.airline_iata",
        "flight.codeshared.flight_number",
        "flight.codeshared.flight_iata",
    ]

    transformed_data = flattened_data[
        [c for c in required_columns if c in flattened_data.columns]
    ]
    transformed_data.columns = [
        c.replace(".", "_") for c in transformed_data.columns
    ]

    transformed_data["ingested_at"] = ingested_at

    return transformed_data


def save_transformed_flights(transformed_data, filename, parent_directory):
    try:
        # 1. Ensure directory exists
        output_dir = Path(f"data/transformed/flights/{parent_directory}")
        output_dir.mkdir(parents=True, exist_ok=True)

        # 2. Save parquet file to directory.
        filename_ext = f"{filename}_transformed.parquet"
        full_path = output_dir / filename_ext
        transformed_data.to_parquet(full_path, index=False)

    except (TypeError, PermissionError, OSError) as e:
        logging.error(f"Failed to create output directory. {e}")
        raise        

    except AttributeError as e:
        logging.error(f"Tried to save a non-valid dataframe.")
        raise

    except Exception as e:
        logging.error(f"An unexpected error occured. {e}")
        raise


def run_transformation(raw_data_path):
    data, filename, parent_directory = load_raw_flights_data(raw_data_path)
    transformed_data = transform_raw_flight_data(data)
    save_transformed_flights(transformed_data, filename, parent_directory)