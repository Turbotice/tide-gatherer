# /usr/bin/env python3

"""Collect tide information from marees.gc.ca"""

import enum
import datetime
import pathlib
import polars as pl
import pytz
import requests


class Resolution(enum.Enum):
    ONE_MINUTE = 1
    THREE_MINUTES = 3
    FIVE_MINUTES = 5
    FIFTEEN_MINUTES = 15
    SIXTY_MINUTES = 60


rimouski_id = "5cebf1e03d0f4a073c4bbd92"
data_code = "wlo"
server_url = "https://api.iwls-sine.azure.cloud-nuage.dfo-mpo.gc.ca"
tz_str = "America/Montreal"

url = server_url + f"/api/v1/stations/{rimouski_id}/data"

year = 2024


def make_df(year, month, day, timezone, resolution: Resolution):
    tz = pytz.timezone("America/Montreal")

    dfrom, dto = date_to_iso(year, month, day, timezone)

    payload = {
        "time-series-code": data_code,
        "resolution": resolution.name,
        "from": dfrom,
        "to": dto,
    }

    r = requests.get(url, params=payload)
    df = pl.from_dict(
        dict(
            zip(
                ("date", "tide_height"),
                zip(
                    *map(
                        lambda _d: (
                            _d["eventDate"],
                            _d["value"] if _d["reviewed"] else None,
                        ),
                        r.json(),
                    )
                ),
            )
        )
    )
    df = df.with_columns(pl.col("date").str.to_datetime().dt.convert_time_zone(tz.zone))

    print(make_filename(dfrom, resolution))
    return df


def make_filename(dfrom, resolution):
    return dfrom.split("T")[0] + f"_r{resolution.value:02d}m_tides.csv"


def date_to_iso(year, month, day, timezone="America/Montreal"):
    tz = pytz.timezone(timezone)

    return [
        tz.localize(
            datetime.datetime.strptime(f"{year}-{month}-{day} {time}", "%Y-%m-%d %H:%M")
        ).isoformat()
        for time in ("00:00", "23:59")
    ]


def write_file(): ...


def discover(path: pathlib.Path):
    for _dir in sorted(path.iterdir()):
        stem = _dir.stem
        if len(stem) == 4 and _dir.is_dir():
            try:
                int(stem)
            except ValueError:
                continue

            month, day = map(int, (stem[:2], stem[2:]))
            dfrom, dto = date_to_iso(year, month, day, timezone)

            df = make_df(year, month, day, timezone, resolution)
            filename = make_filename
