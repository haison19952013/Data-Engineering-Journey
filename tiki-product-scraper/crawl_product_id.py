import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
import re
import pandas as pd


async def main():
    config = CrawlerRunConfig(
        css_selector = "#__next > div:nth-child(2) > main > div > div > div.sc-dfad4f1d-0.dHZHvj > div.sc-dfad4f1d-3.TjGns > div.sc-92fcafd8-0.fLLNCa"
        
    
    )
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url="https://tiki.vn/ngon/c44792"
        )
    print(result.status_code)
    return str(result.markdown)

if __name__ == "__main__":
    markdown = asyncio.run(main())
    print(markdown)