import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
import re
import pandas as pd

def md2csv(md_text):
    # Regular expression pattern to extract group name and URL
    pattern = re.compile(r"\[(.*?)\]\((https://tiki.vn/[^\s]+)")

    # Extract data
    matches = pattern.findall(md_text)
    
    df = pd.DataFrame(matches, columns=["category", "url"])

    # Save to CSV
    csv_filename = "data/product_category.csv"
    df.to_csv(csv_filename, index=False)

    print(f"CSV file saved: {csv_filename}")


async def main():
    config = CrawlerRunConfig(
        css_selector = "#__next > div.home-page > main > div > div > div.sc-2c6b9fc9-0.eEuHKG > div:nth-child(1)"
    
    )
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url="https://tiki.vn", 
            config=config
        )

    return str(result.markdown)

if __name__ == "__main__":
    markdown = asyncio.run(main())
    md2csv(markdown)