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
    format="%(asctime)s [%(levelname)s] %(message)s"
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

# Directories
FUELPRICES_DIR = "fuelprices"
PARSED_FUELPRICES_DIR = "docs/fuelprices_json"
WEBPAGE_ROOT = "docs"

# Ensure directories exist
os.makedirs(FUELPRICES_DIR, exist_ok=True)
os.makedirs(PARSED_FUELPRICES_DIR, exist_ok=True)

async def save_site_mappings(site_mappings):
    """
    Save site mappings to a JSON file.
    """
    mappings_filepath = os.path.join(WEBPAGE_ROOT, "site_mappings.json")
    async with aiofiles.open(mappings_filepath, "w") as f:
        await f.write(json.dumps(site_mappings, indent=4))
    print(f"Saved site mappings to {mappings_filepath}")

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
    Fetch site mappings from `get_sites` and `site`.
    """
    async with aiohttp.ClientSession() as session:
        # Fetch responses
        get_sites_response = await fetch_json(session, BASE_URLS["get_sites"])
        site_response = await fetch_json(session, BASE_URLS["get_site"])

        # Validate `get_sites` response structure
        if not isinstance(get_sites_response, dict) or "sites" not in get_sites_response:
            logging.error(f"Unexpected structure in get_sites response: {type(get_sites_response)}")
            return set(), {}

        # Validate `site` response structure
        if not isinstance(site_response, list):
            logging.error(f"Unexpected structure in site response: {type(site_response)}")
            return set(), {}

        site_mappings = {}
        site_codes_set = set()

        # Process `get_sites` response
        for site in get_sites_response["sites"]:
            site_code = site.get("site_code")
            if site_code:
                site_mappings[site_code] = {
                    "name": site.get("name", f"Site {site_code}"),
                    "latitude": site.get("latitude"),
                    "longitude": site.get("longitude"),
                    "address": site.get("address"),
                }
                site_codes_set.add(site_code)

        # Process `site` response
        for site in site_response:
            site_code = site.get("SiteCode")
            if site_code:
                if site_code not in site_mappings:
                    site_mappings[site_code] = {}
                site_mappings[site_code].update({
                    "name": site.get("SiteName", f"Site {site_code}"),
                    "latitude": site.get("Latitude"),
                    "longitude": site.get("Longitude"),
                    "address": site.get("StreetAddress"),
                })
                site_codes_set.add(site_code)

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
                logging.error(f"Failed to fetch data for site_code {site_code}: {result}")
                continue

            # Save raw API response
            raw_filename = f"fuelprices_{site_code}.json"
            raw_filepath = os.path.join(FUELPRICES_DIR, raw_filename)
            async with aiofiles.open(raw_filepath, "w") as f:
                await f.write(json.dumps(result, indent=4))
            logging.info(f"Raw API response for site_code {site_code} saved to {raw_filepath}")

            # Initialize site mapping details
            site_details = site_mappings.get(site_code, {"name": f"Site {site_code}"})
            site_name = site_details.get("name")
            latitude = site_details.get("latitude")
            longitude = site_details.get("longitude")

            # Prepare new prices array
            new_prices = []
            for entry in result.get("sitefuelprices", []):
                department_code = entry.get("department_code")
                price = entry.get("current_price")
                date = convert_date(entry["date_entered"])
                if department_code and price and date:
                    new_prices.append({
                        "department_code": department_code,
                        "date": date,
                        "price": price,
                    })

            # Load existing data if available
            parsed_filename = f"fuelprices_{site_code}.json"
            parsed_filepath = os.path.join(PARSED_FUELPRICES_DIR, parsed_filename)
            try:
                async with aiofiles.open(parsed_filepath, "r") as f:
                    existing_data = json.loads(await f.read())
                    existing_prices = existing_data.get("prices", [])
            except FileNotFoundError:
                existing_data = {
                    "site_code": site_code,
                    "site_name": site_name,
                    "latitude": latitude,
                    "longitude": longitude,
                    "prices": [],
                }
                existing_prices = []

            # Merge new prices with existing prices (avoid duplicates)
            updated_prices = {f"{p['date']}_{p['department_code']}": p for p in existing_prices + new_prices}
            existing_data["prices"] = list(updated_prices.values())

            # Save updated parsed data back to the file
            async with aiofiles.open(parsed_filepath, "w") as f:
                await f.write(json.dumps(existing_data, indent=4))
            logging.info(f"Parsed fuel prices for site_code {site_code} saved to {parsed_filepath}")

async def main():
    """
    Main function to orchestrate the workflow.
    """
    # Fetch site codes and mappings
    site_codes_set, site_mappings = await fetch_site_mappings()

    if not site_codes_set:
        logging.error("No site codes were retrieved. Exiting.")
        return

    # Save site mappings for the frontend
    await save_site_mappings(site_mappings)

    # Fetch and save fuel prices
    await fetch_and_save_fuel_prices(site_codes_set, site_mappings)

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
