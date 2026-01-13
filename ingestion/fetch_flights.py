# TODO: use pydantic or python-decouple for env variables

import requests
import os
import json
import logging
from datetime import date, datetime
from pathlib import Path
from decouple import config

logging.basicConfig(
    level=logging.INFO,
    filename="fetch_flights.log",
    encoding="utf-8",
    filemode="a",
    style="{",
    format="{asctime} - {levelname} - {message}",
    datefmt="%Y-%m-%d %H:%M",
)

API_KEY = config("FLIGHTS_API_KEY")
BASE_URL = "https://api.aviationstack.com/v1/flights"

params = {"access_key": API_KEY, "dep_iata": "JED", "limit": 100}


def fetch_flight_data():
    try:
        logging.info(f"Requesting flights from {params['dep_iata']}")
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Records received: {len(data.get('data', []))}")

        if "data" not in data:
            raise ValueError("API response missing 'data' field.")

        if not data["data"]:
            logging.warning("API returned zero flights")

    except requests.exceptions.Timeout:
        logging.error(f"Timeout error: The request to {BASE_URL} timed out.")

    except requests.exceptions.ConnectionError:
        logging.error(
            f"Connection error: Could not connect to the server at {BASE_URL}. Check network connectin or URL."
        )

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: An HTTP error occured: {e}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error: An unexpected error occured during the request: {e}")

    except json.JSONDecodeError:
        logging.error(f"JSON Error: Failed to decode JSON from request body.")

    except Exception as e:
        logging.error(f"Error: An unexpected error occured: {e}")

    else:
        logging.info("Successfully fetched data.")
        return data

    finally:
        logging.info(f"API request attempt finished.")


def save_raw_flights(data):
    today = date.today().isoformat()
    timestamp = datetime.now()
    payload = {
        "metadata": {
            "ingested_at": timestamp.isoformat(),
        },
        "data": data,
    }

    try:
        output_dir = Path(f"data/raw/flights/{today}")
        output_dir.mkdir(parents=True, exist_ok=True)

    except (TypeError, PermissionError, OSError) as e:
        logging.error(f"Failed to create output directory. {e}")

    filename = f'{timestamp.strftime("%Y%m%d%H%M%S")}_flights.json'
    full_path = output_dir / filename

    try:
        with open(full_path, "w") as f:
            json.dump(payload, f, indent=2)

    except (PermissionError, OSError) as e:
        logging.error(f"Failed to create output file. {e}")

    except TypeError as e:
        logging.error(f"Payload is not JSON serializable. {e}")

    except KeyboardInterrupt as e:
        logging.error(f"Operation has been interrupted. {e}")

    else:
        logging.info(f"Successfully saved file to {full_path}")
        return full_path


def run_ingestion():
    data = fetch_flight_data()
    if data:
        return save_raw_flights(data)
    return None

def main():
    run_ingestion()


if __name__ == "__main__":
    main()
