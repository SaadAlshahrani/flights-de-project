import requests
import json
from datetime import date, datetime
from decouple import config
from common.logger import setup_logger
from common.storage import write_json

# Setup

logger = setup_logger(__name__, "fetch_flights.log")

API_KEY = config("FLIGHTS_API_KEY")
BASE_URL = "https://api.aviationstack.com/v1/flights"

params = {"access_key": API_KEY, "dep_iata": "JED", "limit": 100}


def fetch_flight_data():
    try:
        logger.info(f"Requesting flights from {params['dep_iata']}")
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Records received: {len(data.get('data', []))}")

        if "data" not in data:
            raise ValueError("API response missing 'data' field.")

        if not data["data"]:
            logger.warning("API returned zero flights")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error: The request to {BASE_URL} timed out.")
        raise

    except requests.exceptions.ConnectionError:
        logger.error(
            f"Connection error: Could not connect to the server at {BASE_URL}. Check network connectin or URL."
        )
        raise

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: An HTTP error occured: {e}")
        raise

    except requests.exceptions.RequestException as e:
        logger.error(f"Error: An unexpected error occured during the request: {e}")
        raise

    except json.JSONDecodeError:
        logger.error(f"JSON Error: Failed to decode JSON from request body.")
        raise

    except Exception as e:
        logger.error(f"Error: An unexpected error occured: {e}")
        raise

    else:
        logger.info("Successfully fetched data.")
        return data

    finally:
        logger.info(f"API request attempt finished.")


def save_raw_flights(data: dict) -> str:
    # Preparing payload
    today = date.today().isoformat()
    timestamp = datetime.now()
    payload = {
        "metadata": {
            "ingested_at": timestamp.isoformat(),
        },
        "data": data,
    }

    # Writing to storage
    filename = f'{timestamp.strftime("%Y%m%d%H%M%S")}_flights.json'
    blob_path = f"raw/flights/ingestion_date={today}/{filename}"

    try:
        write_json(blob_path, payload)
        logger.info(f"Successfully written JSON data to {blob_path}.")

    except Exception as e:
        logger.error(f"Failed to persist data to cloud storage. {e}")
        raise

    return blob_path


def run_ingestion():
    data = fetch_flight_data()
    if data:
        return save_raw_flights(data)
    return None
