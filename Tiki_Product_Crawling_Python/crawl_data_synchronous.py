import requests
import pandas as pd
import time
import os
from utils import clean_text
from tqdm import tqdm
from utils import Logger
import json

logger = Logger('logs/crawl_data_synchronous.log')
    
@logger.log_errors(logger)
def download_site(url, session):
    '''
    Download product information from a given url
    Args:
        url: url of the product
        session: requests session
    Returns:
        product: dictionary containing product information
    '''
    
    headers = {"User-Agent": "Mozilla/5.0"} 
    with session.get(url, headers=headers) as response:
        data = response.json()
        return {'id':data['id'], 'name':data['name'], 'url_key':data['url_key'], 'price':data['price'], 'description': clean_text(data['description']), 'images':data['images']}

def download_all_sites(sites, rate_limit = 50):
    '''
    Download all sites in the list
    Limit the rate of requests to avoid overwhelming the server
    Args:
        sites: list of urls
        limit_rate: minimum time between requests
    Returns:
        data: list of dictionaries containing product information
        errors: list of dictionaries containing error information
    '''
    json_list = []
    with requests.Session() as session:
        for i in range(len(sites)):
            url = sites[i]
            product = download_site(url = url, session = session)
            time.sleep(1/rate_limit)
            if type(product) == dict:
                json_list.append(product)
    return json_list

@logger.log_timestamp(logger)
def main():
    product_id_df = pd.read_csv('data/product_id.csv')
    ids = product_id_df['id'].values.tolist()
    urls = list(map(lambda x: f'https://api.tiki.vn/product-detail/api/v1/products/{x}', ids))
    batch_size = 1000
    for idx in tqdm(range(0, len(urls), batch_size)):
        batch_urls = urls[idx:idx+batch_size]
        json_list = download_all_sites(batch_urls)
        print(len(json_list))
        file_name = f'data/products_info_synchronous_{idx}.json'
        # Save data
        os.makedirs('data', exist_ok=True)  # Create 'data' directory if it doesn't exist
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(json_list, f, ensure_ascii=False, indent=4)
            print(f'Saved data to {file_name}')
        break #  Comment this if you want to download all data
            
if __name__ == '__main__':
    main()


