import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from config import settings
from database import SessionLocal, Timepoint, init_db

logger = logging.getLogger(__name__)


def parse_playtime(time_str: str) -> float:
    """
    Parses strings like '9h 43m' into hours.
    """
    total_hours = 0.0
    time_map = {"h": 1, "m": 1 / 60, "s": 1 / 3600}

    matches = re.findall(r"(\d+)\s*(h|m|s)", time_str, re.I)
    for value, unit in matches:
        total_hours += int(value) * time_map[unit.lower()]

    return total_hours


def to_timepoints(id_to_val: dict, timestamp: datetime) -> list[Timepoint]:
    return [
        Timepoint(series_id=series_id, timestamp=timestamp, value=float(value))
        for series_id, value in id_to_val.items()
    ]


def table_tag_to_df(table_tag) -> pd.DataFrame:
    if not table_tag:
        return None

    rows = []
    tr_tags = table_tag.find_all(["th", "tr"])
    for tr_tag in tr_tags:
        td_tags = tr_tag.find_all(["th", "td"])
        row = []

        for td in td_tags:
            cell_value = None
            img_tag = td.find("img")
            if img_tag:
                if img_tag.get("title"):
                    cell_value = img_tag.get("title")
                elif img_tag.get("alt"):
                    cell_value = img_tag.get("alt")
            if cell_value is None:
                cell_value = td.get_text(strip=True)
            row.append(cell_value)
        if row:
            rows.append(row)

    header_row = rows[0]
    data_rows = rows[1:]
    df = pd.DataFrame(data_rows, columns=header_row)

    return df


def fix_df_header(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    new_columns = list(df.columns)
    for i, col in enumerate(df.columns):
        if pd.isna(col) or str(col).strip() == "":
            unique_values = df.iloc[:, i].dropna().unique()

            # Dedicated / Non-Dedicated => Is Dedicated
            if any("dedicate" in val.lower() for val in unique_values):
                new_columns[i] = "Is Dedicated"
            # Password Protected => Is Password Protected
            elif any("password" in val.lower() for val in unique_values):
                new_columns[i] = "Is Password Protected"
            else:
                new_columns[i] = f"Unknown_{i}"

    df.columns = new_columns
    return df


def save_to_staging_db(timepoints: list[Timepoint]):

    init_db()
    with SessionLocal() as session:
        with session.begin():
            for tp in timepoints:
                session.merge(tp)


async def extract_timestamp(soup: BeautifulSoup) -> datetime:

    p_tag = soup.find("p", class_="cached")
    if not p_tag:
        return None

    pattern = r"(20\d{2}\D{0,2}\d{2}\D{0,2}\d{2}\D{0,2}\d{2}\D{0,2}\d{2}\D{0,2}\d{2}).*?UTC(?![+-]\d)"
    ts_match = re.search(pattern, p_tag.get_text())
    if not ts_match:
        return None

    ts_str = ts_match.group(1)
    digits_only = re.sub(r"\D", "", ts_str)
    return datetime.strptime(digits_only, "%Y%m%d%H%M%S")


async def extract_file(file: Path, timepoints: list[Timepoint]):

    with file.open("r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    ts = await extract_timestamp(soup)
    logger.info(f"Captured timestamp: {ts}.")
    if ts is None:
        logger.error("No timestamp found.")
        return

    id_to_val: dict[int, float] = {}

    # Extract IPv4 / IPv6 counts
    ipv4_count = None
    ipv6_count = None

    main_tag = soup.find("main")
    if main_tag:
        p_tag = main_tag.find("p")
        if p_tag:
            text = p_tag.get_text()
            v4_match = re.search(r"(\d+)\s*IPv4\s*server", text, re.I)
            v6_match = re.search(r"(\d+)\s*IPv6\s*server", text, re.I)
            if v4_match:
                ipv4_count = int(v4_match.group(1))
            if v6_match:
                ipv6_count = int(v6_match.group(1))

    if ipv4_count is not None:
        id_to_val[1] = ipv4_count
    if ipv6_count is not None:
        id_to_val[2] = ipv6_count

    table_tag = soup.find("table")

    if table_tag:
        logger.info("Found table of server list.")
        df = table_tag_to_df(table_tag)
        df = fix_df_header(df)

        dedicated_count = (
            df["Is Dedicated"].str.match(r"dedicate", case=False, na=False)
        ).sum()
        non_dedicated_count = (
            df["Is Dedicated"].str.match(
                r"non.*?dedicate", case=False, na=False
            )
        ).sum()
        password_protected_count = (
            df["Is Password Protected"]
            .str.contains("password", case=False, na=False)
            .sum()
        )

        id_to_val[3] = dedicated_count
        id_to_val[4] = non_dedicated_count
        id_to_val[5] = password_protected_count
        id_to_val[24] = dedicated_count + non_dedicated_count

        df["play_time_hours"] = df["Play time"].apply(parse_playtime)

        # Parse client counts/capacities
        clients_split = df["Clients"].str.split("/", expand=True)
        if clients_split.shape[1] == 2:
            df["clients_current"] = pd.to_numeric(
                clients_split[0].str.strip(), errors="coerce"
            )
            df["clients_max"] = pd.to_numeric(
                clients_split[1].str.strip(), errors="coerce"
            )
        else:
            df["clients_current"] = pd.NA
            df["clients_max"] = pd.NA

        # Parse company counts/capacities
        companies_split = df["Companies"].str.split("/", expand=True)
        if companies_split.shape[1] == 2:
            df["companies_current"] = pd.to_numeric(
                companies_split[0].str.strip(), errors="coerce"
            )
            df["companies_max"] = pd.to_numeric(
                companies_split[1].str.strip(), errors="coerce"
            )
        else:
            df["companies_current"] = pd.NA
            df["companies_max"] = pd.NA

        # Play Time Metrics
        pt_series = df["play_time_hours"].dropna()
        if not pt_series.empty:
            id_to_val[6] = round(pt_series.max(), 5)
            id_to_val[7] = round(pt_series.min(), 5)
            id_to_val[8] = round(pt_series.mean(), 5)
            id_to_val[9] = round(pt_series.median(), 5)

        # Clients Metrics
        cc_series = df["clients_current"].dropna()
        if not cc_series.empty:
            id_to_val[10] = cc_series.sum()
            id_to_val[11] = cc_series.max()
            id_to_val[12] = cc_series.min()
            id_to_val[13] = round(cc_series.mean(), 5)
            id_to_val[14] = round(cc_series.median(), 5)

        ccap_series = df["clients_max"].dropna()
        if not ccap_series.empty:
            id_to_val[15] = ccap_series.sum()
            id_to_val[16] = round(ccap_series.median(), 5)

        # Companies Metrics
        comp_series = df["companies_current"].dropna()
        if not comp_series.empty:
            id_to_val[17] = comp_series.sum()
            id_to_val[18] = comp_series.max()
            id_to_val[19] = comp_series.min()
            id_to_val[20] = round(comp_series.mean(), 5)
            id_to_val[21] = round(comp_series.median(), 5)

        compcap_series = df["companies_max"].dropna()
        if not compcap_series.empty:
            id_to_val[22] = compcap_series.sum()
            id_to_val[23] = round(compcap_series.median(), 5)
    print(id_to_val)
    adding_tps = to_timepoints(id_to_val, ts)
    timepoints.extend(adding_tps)


async def extract():
    timepoints: list[Timepoint] = []
    for downloaded_file in settings.DOWNLOAD_DIR.rglob("*"):
        for file_key in settings.FILES.keys():
            if downloaded_file.name == file_key:
                await extract_file(downloaded_file, timepoints)
    unique_series_ids = list({tp.series_id for tp in timepoints})
    logger.info(
        f"Staging {len(timepoints)} timepoints. Series ids: {unique_series_ids}."
    )
    save_to_staging_db(timepoints)


if __name__ == "__main__":
    asyncio.run(extract())
