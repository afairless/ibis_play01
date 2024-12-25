#! /usr/bin/env python3

import ibis
import polars as pl
from pathlib import Path


def dummy_function01() -> int:
    """
    This is a function
    """

    return 1


def dummy_function02(input: int) -> int:
    """
    This is a function
    """

    assert input > 0

    return input + 1


def main():
    """
    adapted from:
        https://ibis-project.org/tutorials/getting_started
    """

    project_path = Path.cwd()
    input_path = project_path / 'input'
    input_filepaths = list(input_path.glob('*.parquet'))

    input_filepath_1 = input_filepaths[0]
    input_filepath_2 = input_filepaths[1]
    
    df_1 = pl.read_parquet(input_filepath_1)
    df_2 = pl.read_parquet(input_filepath_2)

    conn = ibis.connect('duckdb://project.ddb')

    conn.create_table('dv', df_1, overwrite=True)
    conn.create_table('rv', df_2, overwrite=True)

    # conn.list_tables()

    dv_df = conn.table('dv')
    rv_df = conn.table('rv')



if __name__ == '__main__':
    main()
