#! /usr/bin/env python3

import ibis
from ibis import _

import polars as pl
import pandas as pd
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


def modf(t):
    return t.x % 3


def main():
    """
    adapted from:
        https://ibis-project.org/tutorials/getting_started
    """

    ibis.options.interactive = True
    ibis.options.verbose = True

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

    dv_tbl = conn.table('dv')
    rv_tbl = conn.table('rv')

    dv_colnames = dv_tbl.columns
    rv_colnames = rv_tbl.columns


    # filtering, selecting, ordering
    ##################################################

    col_idx = [0, 2, 3, 8, -1]
    dv_colname_list = [dv_colnames[idx] for idx in col_idx]
    dv_spy_tbl = (
        dv_tbl
        # filter conditions show two ways of specifying columns
        .filter((dv_tbl.symbol=='SPY') & (dv_tbl[dv_colname_list[2]] > 1e7))
        .select(dv_colname_list)
        # sort by descending order; otherwise, row order is not guaranteed
        .order_by(ibis.desc(dv_colname_list[1])))

    dv_spy_tbl.count()


    # mutate
    ##################################################

    add_col_tbl = (
        dv_spy_tbl
        .order_by(dv_colname_list[-1])
        .mutate(col_minus_one=dv_spy_tbl[dv_colname_list[3]] - 1))

    add_col_tbl.to_polars()


    # chaining
    ##################################################
    # adapted from:
    #   https://ibis-project.org/how-to/analytics/chain_expressions
    ##################################################

    df1 = pd.DataFrame({'x': range(5), 'y': list('ab')*2 + list('e')})
    t1 = ibis.memtable(df1)

    df2 = pd.DataFrame({'x': range(10), 'z': list(reversed(list('ab')*2 + list('e')))*2})
    t2 = ibis.memtable(df2)

    xmod = modf(_)

    ymax = _.y.max()
    zmax = _.z.max()
    zct = _.z.count()

    join = (
        t1
        # _ is t1
        .join(t2, _.x == t2.x)
        # `xmod` is a deferred expression:
        .mutate(xmod=xmod)
        # _ is the TableExpression after mutate:
        .group_by(_.xmod)
        # `ymax` and `zmax` are ColumnExpressions derived from a deferred expression:
        .aggregate(ymax=ymax, zmax=zmax)
        # _ is the aggregation result:
        .filter(_.ymax == _.zmax)
        # _ is the filtered result, and re-create xmod in t2 using modf:
        .join(t2, _.xmod == modf(t2))
        # _ is the second join result:
        .join(t1, _.xmod == modf(t1))
        # _ is the third join result:
        .select(_.x, _.y, _.z)
        # Finally, _ is the selection result:
        .order_by(_.x))


if __name__ == '__main__':
    main()
