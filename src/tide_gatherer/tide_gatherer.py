# /usr/bin/env python3

"""Collect tide information from marees.gc.ca"""

import enum
import datetime
import pathlib
import polars as pl
import pytz
import requests
from typing import Callable, Iterator


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


def work(
    year: int,
    resolution: Resolution,
    paths: list[pathlib.Path],
    **kwargs,
):
    print(kwargs)
    for _path in paths:
        stem = _path.stem
        month, day = str_to_date(stem)
        start_time, end_time = date_to_iso(year, month, day, tz_str)

        json = get_json(start_time, end_time, resolution, **kwargs)
        df = make_df(json, tz_str, **kwargs)

        filename = make_filename(start_time, resolution)
        path = _path.joinpath("Marees")
        make_path(path, **kwargs)
        filepath = path.joinpath(filename)
        write_file(df, filepath, **kwargs)


def make_path(path: pathlib.Path, **kwargs):
    if kwargs["verbose"]:
        print(f"Creating directory {path}")
    if not kwargs["dry_run"]:
        if not path.exists():
            path.mkdir(parents=True)


def str_to_date(date: str) -> Iterator[tuple[int, int]]:
    """Turn a mmdd string into a pair of mm, dd ints"""
    return map(int, (date[:2], date[2:]))


def get_json(
    start_time: str, end_time: str, resolution: Resolution, **kwargs
) -> list[dict[str, str | float | bool]]:
    payload = {
        "time-series-code": data_code,
        "resolution": resolution.name,
        "from": start_time,
        "to": end_time,
    }
    if kwargs["verbose"]:
        date = datetime.datetime.fromisoformat(start_time).date().isoformat()
        print(
            f"Sending request for tide data on {date} "
            f"with resolution {resolution.value} minutes"
        )

    r = requests.get(url, params=payload)
    if not r.status_code == 200:
        raise ValueError("Request error")
    return r.json()


def make_df(
    json_content: list[dict[str, str | float | bool]], timezone: str, **kwargs
) -> pl.DataFrame:
    if kwargs["verbose"]:
        print("Building dataframe")
    tz = pytz.timezone(timezone)
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
                        json_content,
                    )
                ),
            )
        )
    )
    df = df.with_columns(pl.col("date").str.to_datetime().dt.convert_time_zone(tz.zone))
    return df


def make_filename(dfrom: str, resolution: Resolution) -> str:
    return dfrom.split("T")[0] + f"_r{resolution.value:02d}m_tides.csv"


def date_to_iso(year: int, month: int, day: int, timezone: str) -> list[str]:
    tz = pytz.timezone(timezone)
    return [
        tz.localize(
            datetime.datetime.strptime(f"{year}-{month}-{day} {time}", "%Y-%m-%d %H:%M")
        ).isoformat()
        for time in ("00:00", "23:59")
    ]


def write_file(df: pl.DataFrame, path: pathlib.Path, **kwargs):
    if kwargs["verbose"]:
        print(f"Writing dataframe to {path}")
    if not kwargs["dry_run"]:
        df.write_csv(path)
