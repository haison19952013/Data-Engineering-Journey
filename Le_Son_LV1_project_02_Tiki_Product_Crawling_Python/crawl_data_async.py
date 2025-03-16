import pandas as pd
import asyncio
import aiohttp
import os
from utils import clean_text
from utils import Logger
import json
from tqdm import tqdm

logger = Logger('logs/crawl_data_async.log')

# Limit the number of concurrent requests
RATE_LIMIT = 50  # Maximum number of requests per second
SEM_LIMIT = 10  # Adjust this value based on API rate limits

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
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                await asyncio.sleep(1/RATE_LIMIT)
                return {
                        'id': data['id'],
                        'name': data['name'],
                        'url_key': data['url_key'],
                        'price': data['price'],
                        'description': clean_text(data['description']),
                        'images': data['images']
                }
    except Exception as e:
        error_message = f'Error in download_site(url={url}, session={session}): {str(e)}'
        logger.error(error_message)
        return None

async def download_all_sites(sites):
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(download_site(url, session, semaphore)) for url in sites]
           
        # Wait for all tasks to complete concurrently
        tasks = await asyncio.gather(*tasks, return_exceptions=False)
    json_list = [task for task in tasks if task is not None]
    return json_list

@logger.log_timestamp(logger)
def main():
    product_id_df = pd.read_csv('data/product_id.csv')
    ids = product_id_df['id'].values.tolist()
    batch_size = 1000
    urls = list(map(lambda x: f'https://api.tiki.vn/product-detail/api/v1/products/{x}', ids))
    for idx in tqdm(range(0, len(urls), batch_size)):
        batch_urls = urls[idx:idx+batch_size]
        loop = asyncio.get_event_loop()
        json_list = loop.run_until_complete(download_all_sites(batch_urls))
        print(len(json_list))
        file_name = f'data/products_info_async_{idx}.json'
        # Save data
        os.makedirs('data', exist_ok=True)  # Create 'data' directory if it doesn't exist
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(json_list, f, ensure_ascii=False, indent=4)
            print(f'Saved data to {file_name}')
        break #  Comment this if you want to download all data

if __name__ == '__main__':
    main()