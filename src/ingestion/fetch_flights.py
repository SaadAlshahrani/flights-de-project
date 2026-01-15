import requests
import json
from datetime import date, datetime
from pathlib import Path
from decouple import config
from common.logger import setup_logger

# Setup

logger = setup_logger(__name__, "fetch_flights.log")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
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
        output_dir = DATA_DIR / "raw" / "flights" / today
        output_dir.mkdir(parents=True, exist_ok=True)

    except (TypeError, PermissionError, OSError) as e:
        logger.error(f"Failed to create output directory. {e}")
        raise

    filename = f'{timestamp.strftime("%Y%m%d%H%M%S")}_flights.json'
    full_path = output_dir / filename

    try:
        with open(full_path, "w") as f:
            json.dump(payload, f, indent=2)

    except (PermissionError, OSError) as e:
        logger.error(f"Failed to create output file. {e}")
        raise

    except TypeError as e:
        logger.error(f"Payload is not JSON serializable. {e}")
        raise

    except KeyboardInterrupt as e:
        logger.error(f"Operation has been interrupted. {e}")
        raise

    else:
        logger.info(f"Successfully saved file to {full_path}")
        return full_path


def run_ingestion():
    data = fetch_flight_data()
    if data:
        return save_raw_flights(data)
    return None
