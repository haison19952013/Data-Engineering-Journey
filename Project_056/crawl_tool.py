import pandas as pd
import asyncio
import aiohttp
from utils import Logger
from tqdm import tqdm
from bs4 import BeautifulSoup
import random
import ssl
from playwright.async_api import async_playwright

# Config
RATE_LIMIT = 10
SEM_LIMIT = 5
REQUEST_TIMEOUT = 30  # Add timeout constant
PLAYWRIGHT_TIMEOUT = 20000
logger = Logger()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

async def download_with_playwright(url):
    """Download page content using Playwright browser automation."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = await context.new_page()
            
            await page.goto(url, timeout=PLAYWRIGHT_TIMEOUT)
            await page.wait_for_selector("span.base", timeout=5000)

            prod_name = await page.locator("span.base").inner_text()
            language_code = await page.get_attribute("html", "lang")

            await browser.close()
            return {
                "name": prod_name.strip() if prod_name else "fail",
                "language_code": language_code,
                "status_code": "playwright_success",
                "method": "playwright"
            }
    except Exception as e:
        logger.error(f"[Playwright FAIL] {url} - {e}")
        return {
            "name": "fail",
            "language_code": "fail",
            "status_code": "playwright_fail",
            "method": "playwright"
        }

async def download_with_aiohttp(url, session, semaphore):
    """Download page content using aiohttp with semaphore control."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.get(url, headers=headers, ssl=ssl_context, timeout=timeout) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                
                # More robust element finding
                base_element = soup.find("span", class_="base")
                prod_name = base_element.get_text(strip=True) if base_element else "fail"
                
                html_tag = soup.find("html")
                language_code = html_tag.get("lang") if html_tag else None
                
                await asyncio.sleep(1 / RATE_LIMIT)
                return {
                    'name': prod_name,
                    'language_code': language_code,
                    'status_code': response.status,
                    'method': 'aiohttp'
                }
    except asyncio.TimeoutError:
        logger.error(f"[TIMEOUT] {url}")
        return {
            'name': 'fail',
            'language_code': 'fail',
            'status_code': 'timeout',
            'method': 'aiohttp'
        }
    except Exception as e:
        logger.error(f"[ERROR] download_site(url={url}): {e}")
        return {
            'name': 'fail',
            'language_code': 'fail',
            'status_code': 'error',
            'method': 'aiohttp'
        }

async def download_product(product_id, urls, session, semaphore):
    """Try to download product info from multiple URLs, fallback to Playwright if needed."""
    # Try aiohttp first for each URL
    for url in urls:
        result = await download_with_aiohttp(url, session, semaphore)
        if result["name"] != "fail":
            logger.info(f"[SUCCESS] {product_id} - {url}")
            result.update({"product_id": product_id, "url": url})
            return result

    # All aiohttp attempts failed, try Playwright
    logger.warning(f"[AIOHTTP FAIL] Trying Playwright for {product_id}")
    for url in urls:
        result = await download_with_playwright(url)
        if result["name"] != "fail":
            logger.info(f"[SUCCESS via Playwright] {product_id} - {url}")
            result.update({"product_id": product_id, "url": url})
            return result

    # All methods failed
    logger.error(f"[TOTAL FAIL] {product_id}")
    return {
        "name": "fail",
        "language_code": "fail",
        "status_code": "total_fail",
        "method": "none",
        "product_id": product_id,
        "url": urls[0] if urls else "no_url"
    }

async def download_all_sites(prod_urls_dict):
    """Download all products concurrently with rate limiting."""
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(download_product(pid, urls, session, semaphore))
            for pid, urls in prod_urls_dict.items()
        ]
        # Add progress bar
        results = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Downloading"):
            result = await task
            results.append(result)
        return results

def crawl_prod_name(df):
    """Main function to crawl product names from URLs in DataFrame."""
    if df.empty:
        logger.warning("Input DataFrame is empty")
        return pd.DataFrame()
    
    # Validate required columns
    required_cols = ['product_id', 'current_url']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Group URLs by product_id
    prod_urls_dict = df.groupby('product_id')['current_url'].apply(list).to_dict()
    logger.info(f"Processing {len(prod_urls_dict)} unique products")
    
    # Download all products
    results = asyncio.run(download_all_sites(prod_urls_dict))
    
    # Convert results to DataFrame
    result_df = pd.DataFrame(results)
    
    # Log summary
    success_count = len(result_df[result_df['name'] != 'fail'])
    logger.info(f"Successfully scraped {success_count}/{len(result_df)} products")
    
    return result_df