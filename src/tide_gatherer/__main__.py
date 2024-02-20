# /usr/bin/env python3

"""Collect tide information from marees.gc.ca"""

import argparse
import datetime
import pathlib

from .tide_gatherer import str_to_date
from .tide_gatherer import Resolution


def check_date(year, dates):
    for date in dates:
        try:
            int(date)
        except ValueError:
            raise ValueError("Dates should be sequences of four integers")
        month, day = str_to_date(date)
        try:
            datetime.datetime(year, month, day)
        except ValueError:
            raise ValueError("The combination year, month, day should be a valid date")


def check_path(path: pathlib.Path):
    if not (path.exists() and path.is_dir()):
        raise ValueError("The provided path should be an existing directory")


def main(args):
    check_path(args.data_path)
    if args.target is not None:
        check_date(args.year, args.target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_path", type=pathlib.Path)
    parser.add_argument(
        "--year",
        type=int,
        nargs="?",
        default=2024,
        help="Year",
    )
    mode = parser.add_argument_group("Mode", "TODO desc")
    exclusive_group = mode.add_mutually_exclusive_group(required=True)
    exclusive_group.add_argument("--discover", action="store_true", help="TODO")
    exclusive_group.add_argument("--target", type=str, nargs="+", help="TODO")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="TODO",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="TODO",
    )
    parser.add_argument(
        "--resolution",
        type=int,
        nargs="?",
        default=1,
        choices=[e.value for e in Resolution],
        help="TODO",
    )
    args = parser.parse_args()
    main(args)
