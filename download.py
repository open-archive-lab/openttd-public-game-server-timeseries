import asyncio
import logging
import traceback
from pathlib import Path

from playwright.async_api import async_playwright, Browser
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)


async def is_same_as_file(new_content: bytes, file: Path) -> bool:
    if not file.exists():
        return False
    existing_content = file.read_bytes()
    return new_content == existing_content


async def clean_html(content: str) -> bytes:
    soup = BeautifulSoup(content, "lxml")
    for element in soup(["head", "script", "style", "meta", "link"]):
        element.decompose()
    formatted_html = soup.prettify()
    return formatted_html.encode("utf-8")


async def download_html(browser: Browser, url: str, file_name: str):
    page = await browser.new_page()
    try:
        logger.info(f"Navigating to {url}.")
        await page.goto(url)

        raw_content = await page.content()
        logger.info(f"Response size {len(raw_content)}.")

        cleaned_content = await clean_html(raw_content)
        logger.info(f"Cleaned size {len(cleaned_content)}.")

        archive_path = settings.ARCHIVE_DIR / file_name
        if await is_same_as_file(cleaned_content, archive_path):
            logger.info(f"Skipped {file_name} (no changes).")
        else:
            download_path = settings.DOWNLOAD_DIR / file_name
            settings.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            download_path.write_bytes(cleaned_content)
            logger.info(f"Saved {file_name}.")

    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        traceback.print_exc()
    finally:
        await page.close()


async def download():
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)

        tasks = []
        for file_name, file_params in settings.FILES.items():
            url = file_params.get("url")
            if url:
                tasks.append(download_html(browser, url, file_name))

        await asyncio.gather(*tasks)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(download())
