import asyncio
import logging

from clean import clean
from download import download
from extract import extract
from merge import merge

logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting pipeline.")

    logger.info("Downloading data.")
    await download()

    logger.info("Extracting data.")
    await extract()

    logger.info("Merging data.")
    merge()

    logger.info("Cleaning up.")
    clean()

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    asyncio.run(main())
