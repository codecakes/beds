# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

import logging
import time
import functools
import bs4
import scraperwiki
from bs4 import BeautifulSoup
from collections import namedtuple
from typing import Union, NewType
import sqlalchemy
import json

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

# Url to scan
URL = "https://apps.bbmpgov.in/covidbedstatus/"
I = int
Logger = logging.getLogger(name="multiprocess_log")

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


def scrape_beds():
    html_page = scraperwiki.scrape(URL)
    soup = BeautifulSoup(html_page, "lxml")
    divs = soup.find_all("div", attrs={"id": TBL_DIVS})
    tables = [div.findChild("table") for div in divs]
    unique_keys = ["hid", "category"]
    for doc in map(scrape_tables, tables):
        # print("doc")
        # print(doc)
        try:
            scraperwiki.sqlite.save(
                unique_keys=unique_keys, data=doc, table_name="data"
            )
        except sqlalchemy.exc.InterfaceError as e:
            print(f"error in doc for {doc['category']}")
            # print(doc)
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
    try:
        beds_tuple = BedsTuple(*beds)
    except TypeError as e:
        Logger.info(f"category={category}")
        Logger.info("snum, facility_type, *beds", snum, facility_type, beds)
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


# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

if __name__ == "__main__":
    scrape_beds()
