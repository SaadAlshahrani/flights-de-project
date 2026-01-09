import json
import pandas as pd
from pathlib import Path
from datetime import datetime, date

today = date.today().isoformat()

def load_raw_flights_data(raw_data_path):

    try:
        with open(raw_data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        return raw_data

    except FileNotFoundError:
        print(f"Error: File not found.")
        
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from file.")

def transform_raw_flight_data(raw_data):

    flattened_data = pd.json_normalize(raw_data["data"])

    required_columns = [
    'flight_date',
    'flight_status',
    # Departure
    'departure.airport', 'departure.iata', 'departure.terminal', 'departure.delay', 'departure.scheduled', 'departure.estimated', 'departure.actual',
    # Arrival
    'arrival.airport', 'arrival.iata', 'arrival.terminal', 'arrival.delay', 'arrival.scheduled', 'arrival.estimated', 'arrival.actual',
    # Airline
    'airline.name', 'airline.iata',
    # Flight
    'flight.number', 'flight.iata',
    # Codeshared Flights
    'flight.codeshared.airline_name', 'flight.codeshared.airline_iata', 'flight.codeshared.flight_number', 'flight.codeshared.flight_iata' 
    ]

    transformed_data = flattened_data[[c for c in required_columns if c in flattened_data.columns]]
    transformed_data.columns = transformed_data.columns = [c.replace('.', '_') for c in transformed_data.columns]

    return transformed_data

def save_transformed_flights(transformed_data):
    output_dir = Path(f"data/transformed/flights/{today}")
    output_dir.mkdir(parents=True, exist_ok=True)
    transformed_data.to_csv(output_dir / "flights.csv", index=False)

def main():
    
    raw_data_path = Path(f"data/raw/flights/{today}/flights.json")
    data = load_raw_flights_data(raw_data_path)
    transformed_data = transform_raw_flight_data(data)
    save_transformed_flights(transformed_data)

if __name__ == "__main__":

    main()