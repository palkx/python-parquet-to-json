#!/usr/bin/env python

import pandas
import sys


def process_parquet(path: str, output_path: str) -> None:
    df = pandas.read_parquet(path)
    df.to_json(output_path, orient="records", lines=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("missing parquet file name")

    file_name = sys.argv[1]
    out_file_name = sys.argv[2] if len(sys.argv) >= 3 else "out.json"
    if not file_name.endswith(".parquet"):
        raise ValueError("file should be parquet")

    process_parquet(file_name, out_file_name)
