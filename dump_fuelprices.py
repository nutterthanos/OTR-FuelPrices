import logging
import os
import aiohttp
import asyncio
import aiofiles
import json
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("dump_fuelprices.log"),  # Log to a file
        logging.StreamHandler()  # Also log to console
    ]
)

AUTH_TOKEN_PROD = os.getenv("AUTH_TOKEN_PROD")
if not AUTH_TOKEN_PROD:
    logging.error("AUTH_TOKEN_PROD is not set. Please set it as an environment variable.")
    raise ValueError("AUTH_TOKEN_PROD is required.")

BASE_URLS = {
    "get_sites": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/getSites",
    "get_site": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/site",
    "get_fuel_prices": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/getSiteFuelPrices/{}",
}

HEADERS = {
    "AuthToken": AUTH_TOKEN_PROD,
    "Accept-Encoding": "br",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

FUELPRICES_DIR = "fuelprices"
FUELPRICES_JSON_DIR = "fuelprices_json"

# Ensure directories exist
os.makedirs(FUELPRICES_DIR, exist_ok=True)
os.makedirs(FUELPRICES_JSON_DIR, exist_ok=True)

async def save_site_mappings(site_mappings):
    """
    Save site mappings to a JSON file.
    """
    mappings_filepath = os.path.join(FUELPRICES_DIR, "site_mappings.json")
    with open(mappings_filepath, "w") as f:
        json.dump(site_mappings, f, indent=4)
    print(f"Saved site mappings to site_mappings.json")

def convert_date(ms_date):
    """
    Convert Microsoft JSON date format to ISO 8601.
    """
    try:
        timestamp = int(ms_date.split("(")[1].split("+")[0]) / 1000
        return datetime.utcfromtimestamp(timestamp).isoformat()
    except Exception as e:
        print(f"Error converting date {ms_date}: {e}")
        return None


async def fetch_json(session, url):
    """
    Fetch JSON data from the given URL.
    """
    async with session.get(url, headers=HEADERS) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_site_mappings():
    """
    Fetch site mappings from `get_sites` and `get_site`.
    """
    async with aiohttp.ClientSession() as session:
        # Fetch responses
        get_sites = await fetch_json(session, BASE_URLS["get_sites"])
        site_data = await fetch_json(session, BASE_URLS["get_site"])

        # Initialize mappings
        site_mappings = {}
        site_codes_set = set()

        # Process `get_sites` (list of dictionaries or strings)
        if isinstance(get_sites, list):
            for site in get_sites:
                if isinstance(site, dict):
                    site_code = site.get("SiteCode")
                    site_name = site.get("SiteName", f"Site {site_code}")
                elif isinstance(site, str):
                    site_code = site
                    site_name = f"Site {site_code}"
                else:
                    logging.warning(f"Unexpected site format in get_sites: {site}")
                    continue

                if site_code:
                    site_codes_set.add(site_code)
                    site_mappings[site_code] = {"name": site_name}
        else:
            logging.error(f"Unexpected get_sites structure: {get_sites}")

        # Process `get_site` (list of dictionaries)
        if isinstance(site_data, list):
            for site in site_data:
                if isinstance(site, dict):
                    site_code = site.get("site_code") or site.get("SiteCode")
                    site_name = site.get("name") or site.get("SiteName")
                    latitude = site.get("latitude")
                    longitude = site.get("longitude")
                    address = site.get("address")
                    if site_code:
                        site_codes_set.add(site_code)
                        site_mappings[site_code] = {
                            "name": site_name,
                            "latitude": latitude,
                            "longitude": longitude,
                            "address": address,
                        }
                else:
                    logging.warning(f"Unexpected site format in get_site: {site}")
        else:
            logging.error(f"Unexpected get_site structure: {site_data}")

        logging.info(f"Generated site mappings for {len(site_mappings)} sites.")
        return site_codes_set, site_mappings

async def fetch_and_save_fuel_prices(site_codes, site_mappings):
    """
    Fetch and save fuel prices for each site code.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for site_code in site_codes:
            url = BASE_URLS["get_fuel_prices"].format(site_code)
            tasks.append(fetch_json(session, url))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for site_code, result in zip(site_codes, results):
            if isinstance(result, Exception):
                print(f"Failed to fetch data for site_code {site_code}: {result}")
                continue

            # Save original response
            original_filename = f"{site_code}_fuelprices.json"
            original_filepath = os.path.join(FUELPRICES_DIR, original_filename)
            with open(original_filepath, "w") as f:
                json.dump(result, f, indent=4)
            print(f"Saved original response for site_code {site_code} to {original_filename}")

            # Generate parsed JSON files
            site_name = site_mappings.get(site_code, f"Site {site_code}")
            for entry in result.get("sitefuelprices", []):
                department_code = entry.get("department_code")
                price = entry.get("current_price")
                date = convert_date(entry["date_entered"])

                if department_code and price and date:
                    parsed_filename = f"{department_code}_{site_code}_{date}.json"
                    parsed_filepath = os.path.join(FUELPRICES_JSON_DIR, parsed_filename)

                    file_content = {
                        "site_code": site_code,
                        "site_name": site_name,
                        "department_code": department_code,
                        "prices": [{"date": date, "price": price}],
                    }

                    with open(parsed_filepath, "w") as f:
                        json.dump(file_content, f, indent=4)
                    print(f"Generated parsed JSON for site_code {site_code}, department_code {department_code}")


async def main():
    """
    Main function to orchestrate the workflow.
    """
    # Fetch site codes and mappings
    site_codes_set, site_mappings = await fetch_site_mappings()

    # Save site mappings for the frontend
    await save_site_mappings(site_mappings)

    # Fetch and save fuel prices
    await fetch_and_save_fuel_prices(site_codes_set, site_mappings)

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
