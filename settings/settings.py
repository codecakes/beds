import functools
import os

DB_NAME = "data.sqlite"
DB_NAME_DEFAULT = f"sqlite:///{DB_NAME}"

ENV_DB_NAMES = (
    "SCRAPERWIKI_DATABASE_NAME",
    "MORPH_SCRAPERWIKI_DATABASE_NAME",
    "DATABASE_NAME",
)

# Url to scan
URL = "https://apps.bbmpgov.in/covidbedstatus/"
