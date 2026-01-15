import json
import pandas as pd
from pathlib import Path
from common.logger import setup_logger

logger = setup_logger(__name__, "transform_flights.log")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"

def load_raw_flights_data(raw_payload_path):
    path_obj = Path(raw_payload_path)
    filename = path_obj.stem
    parent_directory = path_obj.parent.name

    try:
        with open(raw_payload_path, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)
        return raw_payload, filename, parent_directory

    except FileNotFoundError:
        logger.error(f"File not found.")
        raise

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from file.")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occured. {e}")
        raise


def transform_raw_flight_data(raw_payload):
    # Basic validation
    metadata = raw_payload.get("metadata")
    if not isinstance(metadata, dict):
        logger.error("Missing or invalid 'metadata' object.")
        raise ValueError("Missing metadata.")
    
    ingested_at = metadata.get("ingested_at")
    if not isinstance(ingested_at, str):
        logger.error("Missing or invalid 'ingested_at' field.")
        raise ValueError("Missing 'ingested_at' field.")
    
    raw_data = raw_payload.get("data")
    if not isinstance(raw_data, dict):
        logger.error("Missing or invalid 'data' section.")
        raise ValueError("Missing 'data'.")
    
    records = raw_data.get("data")
    if not isinstance(records, list):
        logger.error("'data' field must be a list.")
        raise TypeError("Not a list.")
    
    if not records:
        logger.error("No flights records.")
        raise ValueError("Empty records.")

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
        output_dir = DATA_DIR / "transformed" / "flights" / parent_directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # 2. Save parquet file to directory.
        filename_ext = f"{filename}_transformed.parquet"
        full_path = output_dir / filename_ext
        transformed_data.to_parquet(full_path, index=False)
        logger.info("Successfully saved transformed data.")

    except (TypeError, PermissionError, OSError) as e:
        logger.error(f"Failed to create output directory. {e}")
        raise        

    except AttributeError as e:
        logger.error(f"Tried to save a non-valid dataframe.")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occured. {e}")
        raise


def run_transformation(raw_data_path):
    data, filename, parent_directory = load_raw_flights_data(raw_data_path)
    transformed_data = transform_raw_flight_data(data)
    save_transformed_flights(transformed_data, filename, parent_directory)