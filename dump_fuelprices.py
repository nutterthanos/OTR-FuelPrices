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

DATA_DIR = "fuelprices"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)


async def fetch_json(session, url):
    """
    Fetch JSON data from a URL.
    """
    async with session.get(url, headers=HEADERS) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_site_codes():
    """
    Fetch and deduplicate site codes from `get_sites` and `get_site`.
    """
    async with aiohttp.ClientSession() as session:
        # Fetch data from endpoints
        get_sites = await fetch_json(session, BASE_URLS["get_sites"])
        site_data = await fetch_json(session, BASE_URLS["get_site"])

        # Extract site codes from get_sites
        site_codes = {site.get("SiteCode") for site in get_sites if "SiteCode" in site}

        # Extract site codes from get_site (previously `site`)
        if "sites" in site_data:
            site_codes.update(site.get("site_code") for site in site_data["sites"] if "site_code" in site)

        print(f"Retrieved {len(site_codes)} site codes: {site_codes}")
        return list(site_codes)

async def save_json(filename, data):
    """
    Save JSON data to a file.
    """
    filepath = os.path.join(DATA_DIR, filename)
    async with aiofiles.open(filepath, mode="w") as file:
        await file.write(json.dumps(data, indent=4))


async def fetch_and_save_fuel_prices(site_codes):
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
            filename = f"{site_code}_fuelprices.json"
            await save_json(filename, result)


async def main():
    """
    Main function to orchestrate the workflow.
    """
    # Semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(10)  # Adjust the value as needed

    # Fetch site codes
    site_codes = await fetch_site_codes()

    # Fetch and save fuel prices
    await fetch_and_save_fuel_prices(site_codes)


# Run the script
if __name__ == "__main__":
    asyncio.run(main())
