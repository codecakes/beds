import functools
import os

DB_NAME_DEFAULT = "sqlite:///data.sqlite"

ENV_DB_NAMES = (
    "SCRAPERWIKI_DATABASE_NAME",
    "MORPH_SCRAPERWIKI_DATABASE_NAME",
    "DATABASE_NAME",
)

constants = {
    # Url to scan
    "URL": "https://apps.bbmpgov.in/covidbedstatus/",
    "ENV_DB_NAMES": ENV_DB_NAMES,
    "SET_DB_NAME": functools.reduce(
        lambda val1, val2: val1 or val2, [os.getenv(e) for e in ENV_DB_NAMES]
    ),
}
