import pandas as pd
import asyncio
import aiohttp
from utils import Logger
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup
import random

# Limit the number of concurrent requests
RATE_LIMIT = 50  # Maximum number of requests per second
SEM_LIMIT = 10  # Adjust this value based on API rate limits

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:100.0) Gecko/20100101 Firefox/100.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
]
logger = Logger()

async def download_site(url, session, semaphore):
    '''
    Download product information from
    a given url
    Args:
        url: url of the product
        session: aiohttp session
    Returns:
        product: dictionary containing product information
    '''
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    return {'name': 'fail', 'language_code': 'fail'}
                html = await response.text() 
                soup = BeautifulSoup(html, "html.parser")
                prod_name = soup.find("span", class_="base").get_text(strip=True)
                html_tag = soup.find("html")
                language_code = html_tag.get("lang") if html_tag else None
                await asyncio.sleep(1/RATE_LIMIT)
                return {'name': prod_name, 'language_code': language_code}
    except Exception as e:
        error_message = f'Error in download_site(url={url}, session={session}): {str(e)}'
        logger.error(error_message)
        return {'name': 'fail', 'language_code': 'fail'}

async def download_all_sites(sites):
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(download_site(url, session, semaphore)) for url in sites]
           
        # Wait for all tasks to complete concurrently
        tasks = await asyncio.gather(*tasks, return_exceptions=False)
    json_list = [task for task in tasks if task is not None]
    return json_list

def crawl_prod_name(df, batch_size = 1000):
    urls = df['current_url'].values.tolist()
    json_list = []
    for idx in tqdm(range(0, len(urls), batch_size)):
        batch_urls = urls[idx:idx+batch_size]
        loop = asyncio.get_event_loop()
        json_list += loop.run_until_complete(download_all_sites(batch_urls))
    df['product_name'] = [json['name'] for json in json_list]
    df['language_code'] = [json['language_code'] for json in json_list]
    return df
