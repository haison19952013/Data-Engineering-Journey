import json
import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.extraction_strategy import JsonXPathExtractionStrategy

async def extract_crypto_prices_xpath():
    # 1. Minimal dummy HTML with some repeating rows
    dummy_html = """
    <html>
      <body>
        <div class='crypto-row'>
          <h2 class='coin-name'>Bitcoin</h2>
          <span class='coin-price'>$28,000</span>
        </div>
        <div class='crypto-row'>
          <h2 class='coin-name'>Ethereum</h2>
          <span class='coin-price'>$1,800</span>
        </div>
      </body>
    </html>
    """

    # 2. Define the JSON schema (XPath version)
    schema = {
        "name": "Crypto Prices via XPath",
        "baseSelector": "//div[@class='crypto-row']",
        "fields": [
            {
                "name": "coin_name",
                "selector": ".//h2[@class='coin-name']",
                "type": "text"
            },
            {
                "name": "price",
                "selector": ".//span[@class='coin-price']",
                "type": "text"
            }
        ]
    }

    # 3. Place the strategy in the CrawlerRunConfig
    config = CrawlerRunConfig(
        extraction_strategy=JsonXPathExtractionStrategy(schema, verbose=True)
    )

    # 4. Use raw:// scheme to pass dummy_html directly
    raw_url = f"raw://{dummy_html}"

    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(
            url=raw_url,
            config=config
        )

        if not result.success:
            print("Crawl failed:", result.error_message)
            return

        data = json.loads(result.extracted_content)
        print(f"Extracted {len(data)} coin rows")
        if data:
            print("First item:", data[0])

asyncio.run(extract_crypto_prices_xpath())