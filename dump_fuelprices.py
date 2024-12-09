import aiohttp
import asyncio
import aiofiles
import json
import os
from datetime import datetime

auth_token = os.getenv('AUTH_TOKEN_PROD')

MAX_CONCURRENT_REQUESTS = 5  # Adjust this as needed

BASE_URLS = {
    "get_sites": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/getSites",
    "get_site": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/site",
    "get_fuel_prices": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/getSiteFuelPrices/{}",
    "get_departments": "https://ibjdnxs3i2.execute-api.ap-southeast-2.amazonaws.com/motrPrd/getDepartments",
}

HEADERS = {
    "AuthToken": auth_token,
    "Accept-Encoding": "br",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}


async def fetch_json(session, url, semaphore):
    async with semaphore:
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            return await response.json()


async def save_json(filename, data):
    async with aiofiles.open(filename, mode="w") as file:
        await file.write(json.dumps(data, indent=4))


async def fetch_site_codes(semaphore):
    async with aiohttp.ClientSession() as session:
        get_sites = await fetch_json(session, BASE_URLS["get_sites"], semaphore)
        site_data = await fetch_json(session, BASE_URLS["get_site"], semaphore)

        site_codes = set()
        site_codes.update(site.get("site_code") for site in get_sites.get("sites", []))
        site_codes.update(site.get("site_code") for site in site_data.get("sites", []))
        return list(site_codes)


async def fetch_and_save_fuel_prices(site_codes, semaphore):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for site_code in site_codes:
            url = BASE_URLS["get_fuel_prices"].format(site_code)
            tasks.append(fetch_json(session, url, semaphore))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for site_code, result in zip(site_codes, results):
            if isinstance(result, Exception):
                print(f"Failed to fetch data for site_code {site_code}: {result}")
                continue
            await save_json(f"{site_code}_fuelprices.json", result)


async def main():
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    site_codes = await fetch_site_codes(semaphore)
    await fetch_and_save_fuel_prices(site_codes, semaphore)


if __name__ == "__main__":
    asyncio.run(main())
