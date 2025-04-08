# 🛠️ Tiki Product Data Scraper

## 1. 📌 Project Overview

This project involves scraping product data from the **Tiki e-commerce platform** using its public API. The key goals are:

- Download and store detailed product data for **200,000 products** (provided via a list of `product_id`s).
- Normalize and clean the `description` field, which contains embedded HTML.
- Improve performance using asynchronous programming with proper rate-limiting and batching.
- Save the output in `.json` format, with each file containing approximately 1,000 product entries.

---

## 2. ✅ Tasks and Requirements

### ✔️ Completed Tasks

- [x] Load `product_id` list from external source  
  ➤ [🔗 List of Product IDs (OneDrive)](https://1drv.ms/u/s!AukvlU4z92FZgp4xIlzQ4giHVa5Lpw?e=qDXctn)
- [x] Scrape product detail via Tiki API  
  ➤ API Example: [`https://api.tiki.vn/product-detail/api/v1/products/138083218`](https://api.tiki.vn/product-detail/api/v1/products/138083218)
- [x] Extract fields: `id`, `name`, `url_key`, `price`, `description`, `images.url`
- [x] Clean HTML from `description` using `BeautifulSoup`
- [x] Save data into multiple `.json` files (~1000 products/file)
- [x] Implement async scraping using `aiohttp`, `asyncio`, `Semaphore`
- [x] Add fallback synchronous version using `requests`
- [x] Benchmark both methods for performance

### ⏳ Future Tasks

- [ ] Add retry logic for failed requests
- [ ] Add CLI support for output path and batch size
- [ ] Write unit tests for key components
- [ ] Log the number of products processed per batch (e.g., ` logger.info(f"Batch {idx}: {len(json_list)} items downloaded")` to control the crawling process)
- [ ] Catch specific exceptions like `aiohttp.ClientError` and `asyncio.TimeoutError` for better error handling
- [ ] Use `asyncio.run()` instead of `asyncio.get_event_loop()` for better compatibility across environments
- [ ] Write a module to crawl the product IDs from the Tiki website

---

## 3. 💡 Additional Analysis Ideas

- NLP on descriptions to cluster similar products.
- Analyze price ranges, top categories, or trends over time.
- Build dashboards to monitor scraping performance and errors.

---

## 4. 🔧 Approach and Tools Used

### Data Collection & Cleaning
- **Input**: 200,000 `product_id`s
- **Output**: JSON files containing cleaned product data

### Libraries Used
- `aiohttp`, `asyncio`, `requests` — HTTP requests
- `BeautifulSoup` — HTML parsing
- `json`, `tqdm`, `os`, `logging`, `time` — utilities and logging

---

## 5. 🚀 How to Run

### Option 1 — Asynchronous (Recommended)
```bash
python crawl_data_async.py
```

### Option 2 — Synchronous (Slower)
```bash
python crawl_data_synchronous.py
```

---

## 6. 📈 Results

### Runtime Comparison

- **Synchronous approach**:
  - ⏱️ ~5 hours for 200,000 products  

- **Asynchronous approach**:
  - ⏱️ ~1 hour for 200,000 products  

✅ **Conclusion**: Async scraping is ~5x faster than synchronous.

---

## 7. 🧩 Challenges & Solutions

| Challenge | Solution |
|----------|----------|
| API rate limit (max 4500 requests/min) | Applied `asyncio.Semaphore` + delay logic |
| HTML in `description` | Cleaned using `BeautifulSoup` |
| General exception logging in async context | Replaced with specific `aiohttp.ClientError`, `asyncio.TimeoutError` |
| No visibility into batch size | Logged product count per batch with `logger.info(...)` |

---

## 8. 🧠 Lessons Learned

- **Concurrency**: Async I/O is ideal for network-bound tasks; `aiohttp` significantly boosts scraping speed.
- **Exception Handling**: Catching specific errors like `ClientError` or `TimeoutError` improves debugging.
- **Logging**: Monitoring progress via batch-level logging is essential for long-running scrapers.
- **Cross-platform compatibility**: Use `asyncio.run()` over `get_event_loop()` to avoid issues in environments like Jupyter.
- **Robustness**: Always ensure necessary folders (e.g., logs) exist before writing files.

---

## 9. 🔄 Mentor Feedback & Improvements

The following enhancements were made based on expert review:

- ✅ **Good practices**: Used `aiohttp`, batching, and semaphores effectively; rate limiting and session reuse handled well.
- ⚠️ **Improvement**: Replaced `asyncio.get_event_loop()` with `asyncio.run()` for better environment support.
- 🛠️ **Error handling**: Now catching `aiohttp.ClientError` and `asyncio.TimeoutError` explicitly.
- 📁 **Log folder**: Added auto-creation for `logs/` directory to avoid path errors.
- 📊 **Data tracking**: Log product count per batch to ensure completeness and transparency.

---

## 10. 📚 Resources

- [Real Python: Concurrency in Python](https://realpython.com/python-concurrency/)
- [`aiohttp` Documentation](https://docs.aiohttp.org/)
- [`asyncio` Semaphore](https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore)
