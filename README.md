# openttd-public-game-server-timeseries

A Python-based ETL pipeline that automatically scrapes, aggregates, and tracks time-series statistics for OpenTTD public game servers. It fetches data from the [OpenTTD Server Listing](https://servers.openttd.org/listing) and generates historical metrics regarding server counts, connected clients, companies, and server uptimes.

## Quick start

### Install dependencies

Install python dependencies and Playwright Firefox binaries

```bash
pip install -r requirements.txt
playwright install firefox
```

### Run

Run the full data pipeline:

```bash
python main.py
```

### Key Outputs

- **`series.csv`**: The primary database containing all extracted timeseries data.
- **`archives/`**: Directory storing historically processed HTML files.

## Pipeline Overview

Orchestrated by `main.py`, the pipeline runs four stages:

1. **Download (`download.py`)**: Asynchronously fetches and cleans the latest HTML from the OpenTTD listing using Playwright.

2. **Extract (`extract.py`)**: Parses the HTML, calculates various statistical metrics, and saves them to a temporary SQLite staging database.

3. **Merge (`merge.py`)**: Appends the new staging timepoints to a persistent `series.csv` file, resolving any duplicate entries.

4. **Clean (`clean.py`)**: Archives processed HTML files and removes the temporary database

## Configuration

Settings and file paths can be modified in `config.py` or by using a `.env` file.
