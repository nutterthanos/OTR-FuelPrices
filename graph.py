import os
import json
from datetime import datetime
import matplotlib.pyplot as plt

DATA_DIR = "fuelprices"
GRAPH_DIR = "graphs"

def load_fuel_prices():
    """
    Load fuel prices from JSON files into a dictionary.
    """
    fuel_data = {}
    for file in os.listdir(DATA_DIR):
        if file.endswith("_fuelprices.json"):
            site_code = file.split("_")[0]
            filepath = os.path.join(DATA_DIR, file)
            with open(filepath, "r") as f:
                try:
                    data = json.load(f)
                    # Extract sitefuelprices and ensure it's a list
                    sitefuelprices = data.get("sitefuelprices", [])
                    if isinstance(sitefuelprices, list):
                        fuel_data[site_code] = sitefuelprices
                    else:
                        print(f"Unexpected structure for sitefuelprices in {file}")
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in {file}: {e}")
    return fuel_data

if not os.path.exists(GRAPH_DIR):
    os.makedirs(GRAPH_DIR)

def convert_date(ms_date):
    """
    Convert Microsoft JSON date format to Python datetime.
    Example: "/Date(1733180280000+1030)/"
    """
    try:
        timestamp = int(ms_date.split("(")[1].split("+")[0]) / 1000
        return datetime.fromtimestamp(timestamp)
    except Exception as e:
        print(f"Error converting date {ms_date}: {e}")
        return None

def generate_graphs(fuel_data, date_range, department_code):
    """
    Generate graphs for fuel prices for the given department code and date range.
    """
    for site_code, data in fuel_data.items():
        print(f"Generating graph for site_code {site_code}...")
        try:
            prices = [
                entry["current_price"]
                for entry in data
                if entry.get("department_code") == department_code
            ]
            dates = [
                convert_date(entry["date_entered"])
                for entry in data
                if entry.get("department_code") == department_code
            ]

            # Filter out any None values in dates
            if not prices or not dates or None in dates:
                print(f"No valid data for site_code {site_code} with department_code {department_code}")
                continue

            plt.figure()
            plt.plot(dates, prices)
            plt.title(f"Fuel Prices for {site_code} ({department_code})")
            plt.xlabel("Date")
            plt.ylabel("Price")
            plt.grid(True)

            filename = os.path.join(GRAPH_DIR, f"{site_code}_{date_range}_{department_code}.jpg")
            plt.savefig(filename)
            plt.close()
            print(f"Graph saved to {filename}")
        except Exception as e:
            print(f"Error generating graph for site_code {site_code}: {e}")

if __name__ == "__main__":
    fuel_data = load_fuel_prices()
    generate_graphs(fuel_data, "weekly", "diesel")
