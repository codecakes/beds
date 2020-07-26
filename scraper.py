# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

import functools
import json
import logging
import os
import sqlite3
import time
from collections import namedtuple
from typing import Union, NewType

import bs4
import scraperwiki
import sqlalchemy
from bs4 import BeautifulSoup

from settings import settings

I = int
Logger = logging.getLogger(name="multiprocess_log")
Logger.setLevel(logging.DEBUG)

# Set all Env variable keys
Logger.info("setting environ keys")
Logger.info(f"DB_NAME_DEFAULT is {settings.DB_NAME_DEFAULT}")
for env_key in settings.ENV_DB_NAMES:
    os.environ[env_key] = settings.DB_NAME_DEFAULT

StrIntType = NewType("StrIntType", Union[str, int])

NAMED_COLS = (
    "total_gen",
    "total_hdu",
    "total_icu",
    "total_ice_vent",
    "total_allot",
    "filled_gen",
    "filled_hdu",
    "filled_icu",
    "filled_icu_vent",
    "filled_allot",
    "net_gen",
    "net_hdu",
    "net_icu",
    "net_icu_vent",
    "net_allot",
)
COLS_SIZE = len(NAMED_COLS)
BedsTuple = namedtuple("Beds", NAMED_COLS)

# Specific tables are under specific div ids
TBL_DIVS = (
    "private_hospital",
    "private_medical_college",
    "covid_care_centers",
    "governmenthospital",
    "government_medical_college",
)

REPLACE_CHARS = {"(": "", ")": ""}

sanitize_str = lambda line: line.translate(str.maketrans(REPLACE_CHARS))


def create_conn(db_filename: str):
    try:
        conn = sqlite3.connect(db_filename)
    except sqlite3.Error as e:
        Logger.error(e)
        raise e
    finally:
        conn.close()


def scrape_beds():
    html_page = scraperwiki.scrape(settings.URL)
    soup = BeautifulSoup(html_page, "lxml")
    divs = soup.find_all("div", attrs={"id": TBL_DIVS})
    tables = [div.findChild("table") for div in divs]
    unique_keys = ["hid", "category"]
    create_conn(settings.DB_NAME)
    for doc in map(scrape_tables, tables):
        try:
            scraperwiki.sqlite.save(unique_keys=unique_keys, data=doc)
        except sqlalchemy.exc.InterfaceError as e:
            Logger.error(f"error in doc for {doc['category']}")
            raise (e)


def scrape_tables(table: bs4.element.Tag):
    category = sanitize_str(table.findAll("tr")[0].text.strip())
    columns = json.dumps(
        list(map(sanitize_str, table.findAll("tr")[1].stripped_strings)),
        allow_nan=False,
    )
    grand_total = [0]
    grand_available = [0]
    grand_occupied = [0]
    records = []
    doc = {"category": category, "columns": columns}
    start_time = time.time()
    _multi_process_scrape_partial = functools.partial(
        _multi_process_scrape,
        doc,
        records,
        category,
        grand_total,
        grand_available,
        grand_occupied,
    )
    list(map(_multi_process_scrape_partial, table.findAll("tr")[3:]))
    return {
        "grand_total_beds": grand_total[0],
        "grand_occupied_beds": grand_occupied[0],
        "grand_available_beds": grand_available[0],
        "category": category,
        "columns": columns,
        "records": json.dumps(records, allow_nan=False),
        "hid": hash(doc["category"]),
    }


def _multi_process_scrape(
    doc, records, category, grand_total, grand_available, grand_occupied, row
):
    # 'Gen', 'HDU', 'ICU', 'ICU', 'Ventl', 'Total', 'Gen', 'HDU', 'ICU', 'ICU', 'Ventl', 'Total', 'Gen', 'HDU',
    # 'ICU', 'ICU', 'Ventl', 'Total
    snum: StrIntType
    facility_type: StrIntType
    beds: list

    snum, facility_type, *beds = list(row.stripped_strings)
    if facility_type.isdigit():
        beds = [facility_type] + beds
    beds = beds[:COLS_SIZE]
    # some tables may have last field "net_allot" missing
    if len(beds) != COLS_SIZE:
        beds += [sum(I(num) for num in beds[-4:])]
    try:
        beds_tuple = BedsTuple(*beds)
    except TypeError as e:
        Logger.error(f"category={category}")
        Logger.error("snum, facility_type, *beds", snum, facility_type, beds)
        raise e
    if not (snum and snum.isdigit()):
        doc.update(
            {
                "summary": {
                    "sum_total_gen": beds_tuple.total_gen,
                    "sum_total_hdu": beds_tuple.total_hdu,
                    "sum_total_icu": beds_tuple.total_icu,
                    "sum_total_icu_vent": beds_tuple.total_ice_vent,
                    "sum_filled_gen": beds_tuple.filled_gen,
                    "sum_filled_hdu": beds_tuple.filled_hdu,
                    "sum_filled_icu": beds_tuple.filled_icu,
                    "sum_filled_icu_vent": beds_tuple.filled_icu_vent,
                    "sum_net_gen": beds_tuple.net_gen,
                    "sum_net_hdu": beds_tuple.net_hdu,
                    "sum_net_icu": beds_tuple.net_icu,
                    "sum_net_icu_vent": beds_tuple.net_icu_vent,
                }
            }
        )
    else:
        records += [
            {
                "snum": f"{category}_{snum}",
                "facility_type": facility_type,
                "total_gen": beds_tuple.total_gen,
                "total_hdu": beds_tuple.total_hdu,
                "total_icu": beds_tuple.total_icu,
                "total_ice_vent": beds_tuple.total_ice_vent,
                "total_allot": beds_tuple.total_allot,
                "filled_gen": beds_tuple.filled_gen,
                "filled_hdu": beds_tuple.filled_hdu,
                "filled_icu": beds_tuple.filled_icu,
                "filled_icu_vent": beds_tuple.filled_icu_vent,
                "filled_allot": beds_tuple.filled_allot,
                "net_gen": beds_tuple.net_gen,
                "net_hdu": beds_tuple.net_hdu,
                "net_icu": beds_tuple.net_icu,
                "net_icu_vent": beds_tuple.net_icu_vent,
                "net_allot": beds_tuple.net_allot,
                "total_beds": beds_tuple.total_allot,
                "total_occupied_beds": beds_tuple.filled_allot,
                "total_available_beds": beds_tuple.net_allot,
            },
        ]
        grand_total[0] += I(beds_tuple.total_allot)
        grand_occupied[0] += I(beds_tuple.filled_allot)
        grand_available[0] += I(beds_tuple.net_allot)



# Run without checking main module.
scrape_beds()
