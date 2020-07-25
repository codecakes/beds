import functools
import os

DB_NAME_DEFAULT = "sqlite:///data.sqlite"

ENV_DB_NAMES = (
    "SCRAPERWIKI_DATABASE_NAME",
    "MORPH_SCRAPERWIKI_DATABASE_NAME",
    "DATABASE_NAME",
)

SET_DB_NAME = functools.reduce(
    lambda val1, val2: val1 or val2, map(os.getenv, ENV_DB_NAMES)
)
constants = dict(
    # Url to scan
    URL="https://apps.bbmpgov.in/covidbedstatus/",
    ENV_DB_NAMES=ENV_DB_NAMES,
    SET_DB_NAME=SET_DB_NAME,
)
