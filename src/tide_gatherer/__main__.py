# /usr/bin/env python3

"""Collect tide information from marees.gc.ca"""

import argparse
import datetime
import pathlib

from .tide_gatherer import Resolution, Station, str_to_date, work


def check_dates(year, dates):
    for date in dates:
        check_date(year, date)


def check_date(year, date):
    try:
        int(date)
    except ValueError:
        raise ValueError("Dates should be sequences of four integers")
    month, day = str_to_date(date)
    try:
        datetime.date(year, month, day)
    except ValueError:
        raise ValueError("The combination year, month, day should be a valid date")


def check_path(path: pathlib.Path):
    if not (path.exists() and path.is_dir()):
        raise ValueError("The provided path should be an existing directory")


def build_kwargs(args):
    return {k: getattr(args, k) for k in ("dry_run", "verbose", "interactive")}


def parse():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_path", type=pathlib.Path)
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year",
    )
    parser.add_argument(
        "--resolution",
        type=int,
        nargs="?",
        default=1,
        choices=[e.value for e in Resolution],
        help="TODO",
    )
    parser.add_argument(
        "--station",
        type=str,
        nargs="?",
        default="rmsk",
        choices=[e.value for e in Station],
        help="TODO",
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
        "--dry-run",
        action="store_true",
        help="TODO",
    )

    args = parser.parse_args()
    return args
    main(args)


def do_work(args):
    check_path(args.data_path)
    if args.discover:
        targets = list(args.data_path.iterdir())
    else:
        targets = sorted(args.data_path.joinpath(date) for date in args.target)
    for i, target in enumerate(targets):
        try:
            check_date(args.year, target.stem)
        except ValueError:
            targets[i] = None
    targets = sorted(filter(lambda s: s is not None, targets))

    work(
        args.year,
        Resolution(args.resolution),
        Station(args.station),
        targets,
        **build_kwargs(args),
    )


def main():
    args = parse()
    do_work(args)
    return 0


if __name__ == "__main__":
    main()
