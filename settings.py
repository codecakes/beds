import functools
import os

ENV_DB_NAME = (
    "SCRAPERWIKI_DATABASE_NAME",
    "MORPH_SCRAPERWIKI_DATABASE_NAME",
    "DATABASE_NAME",
)
DB_NAME_DEFAULT = "sqlite:///data.sqlite"

constants = dict(
    # Url to scan
    URL="https://apps.bbmpgov.in/covidbedstatus/",
    ENV_DB_NAME=(
        "SCRAPERWIKI_DATABASE_NAME",
        "MORPH_SCRAPERWIKI_DATABASE_NAME",
        "DATABASE_NAME",
    ),
    SET_DB_NAME=functools.reduce(
        lambda key, result: os.getenv(key, None) or result,
        ENV_DB_NAME,
        DB_NAME_DEFAULT,
    ),
)
