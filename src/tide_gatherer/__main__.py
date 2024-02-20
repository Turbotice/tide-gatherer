# /usr/bin/env python3

"""Collect tide information from marees.gc.ca"""

import argparse
import pathlib


def main(args): ...


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
    exclusive_group.add_argument("--discover", help="TODO")
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
        help="TODO",
    )
    args = parser.parse_args()
    main(args)
