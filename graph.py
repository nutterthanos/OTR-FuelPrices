import os
import json
from datetime import datetime
import matplotlib.pyplot as plt

DATA_DIR = "fuelprices"
GRAPH_DIR = "graphs"


def load_fuel_prices():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith("_fuelprices.json")]
    fuel_data = {}
    for file in files:
        with open(os.path.join(DATA_DIR, file), "r") as f:
            data = json.load(f)
            site_code = file.split("_")[0]
            fuel_data[site_code] = data
    return fuel_data


def generate_graphs(fuel_data, date_range, department_code):
    os.makedirs(GRAPH_DIR, exist_ok=True)

    for site_code, data in fuel_data.items():
        prices = [
            entry["price"]
            for entry in data
            if entry["department_code"] == department_code
        ]
        dates = [
            datetime.fromisoformat(entry["date"])
            for entry in data
            if entry["department_code"] == department_code
        ]

        if not prices or not dates:
            continue

        plt.figure()
        plt.plot(dates, prices)
        plt.title(f"Fuel Prices for {site_code}")
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.grid(True)

        filename = os.path.join(
            GRAPH_DIR, f"{site_code}_{date_range}_{department_code}.jpg"
        )
        plt.savefig(filename)
        plt.close()


if __name__ == "__main__":
    fuel_data = load_fuel_prices()
    generate_graphs(fuel_data, "weekly", "diesel")
