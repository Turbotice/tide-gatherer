# /usr/bin/env python3

"""Collect tide information from marees.gc.ca"""

import enum
import datetime
import pathlib
import polars as pl
import pytz
import requests
from typing import Iterator


class Resolution(enum.Enum):
    ONE_MINUTE = 1
    THREE_MINUTES = 3
    FIVE_MINUTES = 5
    FIFTEEN_MINUTES = 15
    SIXTY_MINUTES = 60


class Station(enum.Enum):
    RIMOUSKI = "rmsk"
    BAIE_STE_CATH = "bscath"


IDS = {
    Station.RIMOUSKI: "5cebf1e03d0f4a073c4bbd92",
    Station.BAIE_STE_CATH: "5cebf1e43d0f4a073c4bc427",
}

DATA_CODE = "wlo"
SERVER_URL = "https://api.iwls-sine.azure.cloud-nuage.dfo-mpo.gc.ca"
EST_TZ = "America/Montreal"


def build_url(station: Station):
    return SERVER_URL + f"/api/v1/stations/{IDS[station]}/data"


def work(
    year: int,
    resolution: Resolution,
    station: Station,
    paths: list[pathlib.Path],
    **kwargs,
):
    tz = pytz.timezone(EST_TZ)

    for _path in paths:
        stem = _path.stem
        month, day = str_to_date(stem)
        start_time, end_time = date_to_iso(year, month, day, tz)
        if not (
            datetime.datetime.fromisoformat(end_time)
            < tz.localize(datetime.datetime.today())
        ):
            if kwargs["verbose"]:
                _d = datetime.datetime.fromisoformat(end_time).date()
                print(f"Skipping {_d}: this day is not over yet")
            continue

        json = get_json(start_time, end_time, resolution, station, **kwargs)
        df = make_df(json, tz, **kwargs)

        filename = make_filename(start_time, resolution, station)
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
    start_time: str, end_time: str, resolution: Resolution, station: Station, **kwargs
) -> list[dict[str, str | float | bool]]:
    url = build_url(station)
    payload = {
        "time-series-code": DATA_CODE,
        "resolution": resolution.name,
        "from": start_time,
        "to": end_time,
    }
    if kwargs["verbose"]:
        date = datetime.datetime.fromisoformat(start_time).date().isoformat()
        duration = "minute" if resolution is Resolution.ONE_MINUTE else "minutes"
        print(
            f"---\nSending request to {url} for tide data on {date} "
            f"with resolution {resolution.value} {duration}"
        )

    r = requests.get(url, params=payload)
    if not r.status_code == 200:
        raise ValueError("Request error")
    return r.json()


def make_df(
    json_content: list[dict[str, str | float | bool]], timezone: pytz.tzfile, **kwargs
) -> pl.DataFrame:
    if kwargs["verbose"]:
        print("Building dataframe")
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
    df = df.with_columns(
        pl.col("date").str.to_datetime().dt.convert_time_zone(timezone.zone)
    )
    return df


def make_filename(dfrom: str, resolution: Resolution, station: Station) -> str:
    str_station = f"_{station.value}" if station != station.RIMOUSKI else ""
    return dfrom.split("T")[0] + f"_r{resolution.value:02d}m_tides{str_station}.csv"


def date_to_iso(year: int, month: int, day: int, timezone: pytz.tzfile) -> list[str]:
    return [
        timezone.localize(
            datetime.datetime.strptime(f"{year}-{month}-{day} {time}", "%Y-%m-%d %H:%M")
        ).isoformat()
        for time in ("00:00", "23:59")
    ]


def write_file(df: pl.DataFrame, path: pathlib.Path, **kwargs):
    if kwargs["verbose"]:
        print(f"Writing dataframe to {path}")
    if not kwargs["dry_run"]:
        df.write_csv(path)
