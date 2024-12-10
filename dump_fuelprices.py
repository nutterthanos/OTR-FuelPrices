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
    Fetch site mappings from `get_sites` and `site` endpoints.
    """
    async with aiohttp.ClientSession() as session:
        # Fetch data
        get_sites_response = await fetch_json(session, BASE_URLS["get_sites"])
        site_response = await fetch_json(session, BASE_URLS["get_site"])

        # Log raw responses
        logging.debug(f"Raw get_sites response: {json.dumps(get_sites_response, indent=4)}")
        logging.debug(f"Raw site response: {json.dumps(site_response, indent=4)}")

        # Initialize mappings and site codes set
        site_codes_set = set()
        site_mappings = {}

        # Process get_sites response
        if isinstance(get_sites_response, list):
            for site in get_sites_response:
                if isinstance(site, dict):
                    site_code = site.get("SiteCode") or site.get("site_code")
                    if site_code:
                        site_codes_set.add(site_code)
                        site_mappings[site_code] = {"name": site.get("SiteName")}
                elif isinstance(site, str):
                    site_codes_set.add(site)
                    site_mappings[site] = {"name": f"Site {site}"}
        else:
            logging.error("Unexpected get_sites structure.")

        # Process site response
        if isinstance(site_response, dict) and "sites" in site_response:
            for site in site_response["sites"]:
                if isinstance(site, dict):
                    site_code = site.get("site_code")
                    if site_code:
                        site_codes_set.add(site_code)
                        site_mappings[site_code] = {
                            "name": site.get("name"),
                            "latitude": site.get("latitude"),
                            "longitude": site.get("longitude"),
                            "address": site.get("address"),
                        }

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

            # Save original response
            original_filename = f"{site_code}_fuelprices.json"
            original_filepath = os.path.join(FUELPRICES_DIR, original_filename)
            async with aiofiles.open(original_filepath, "w") as f:
                await f.write(json.dumps(result, indent=4))
            logging.info(f"Saved original response for site_code {site_code}.")

            # Generate parsed JSON
            site_name = site_mappings.get(site_code, {}).get("name", f"Site {site_code}")
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
                        "latitude": site_mappings.get(site_code, {}).get("latitude"),
                        "longitude": site_mappings.get(site_code, {}).get("longitude"),
                        "prices": [{"date": date, "price": price}],
                    }

                    async with aiofiles.open(parsed_filepath, "w") as f:
                        await f.write(json.dumps(file_content, indent=4))
                    logging.info(f"Generated parsed JSON for site_code {site_code}, department_code {department_code}.")

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
