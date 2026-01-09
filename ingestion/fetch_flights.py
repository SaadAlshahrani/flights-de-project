# TODO: use pydantic or python-decouple for env variables
# TODO: refactor code to use main function
# TODO: add ingestion time to data

import requests
import os
import json
import logging

from datetime import date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    filename='app.log',
    encoding='utf-8',
    filemode='a',
    style='{',
    format='{asctime} - {levelname} - {message}',
    datefmt='%Y-%m-%d %H:%M',
)

API_KEY = os.environ['FLIGHTS_API_KEY']
BASE_URL = 'https://api.aviationstack.com/v1/flights'

params = {
    'access_key': API_KEY,
    'dep_iata': 'JED',
    'limit': 100
}

try:
    logging.info(f"Requesting flights from {params['dep_iata']}")
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    logging.info(f"Records received: {len(data.get('data', []))}")

    if "data" not in data:
        raise ValueError("API response missing 'data' field.")
    
    if not data['data']:
        logging.warning("API returned zero flights")

except requests.exceptions.Timeout:
    logging.error(f"Timeout error: The request to {BASE_URL} timed out.")
    
except requests.exceptions.ConnectionError:
    logging.error(f"Connection error: Could not connect to the server at {BASE_URL}. Check network connectin or URL.")

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

    today = date.today().isoformat()
    output_dir = Path(f"data/raw/flights/{today}")
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / 'flights.json', 'w') as f:
        json.dump(data, f, indent=2)

finally:
    logging.info(f"API request attempt finished.")