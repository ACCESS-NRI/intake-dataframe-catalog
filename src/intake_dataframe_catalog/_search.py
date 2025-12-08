# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Stolen and adapted from https://github.com/intake/intake-esm/blob/main/intake_esm/_search.py

from collections.abc import Collection
from re import Pattern
from typing import Any, Union

import pandas as pd
import polars as pl
from numpy import ndarray


def _is_pattern(input: Union[str, Pattern, Collection]) -> bool:
    """
    Check whether the passed value is a pattern

    Parameters
    ----------
    value: str or Pattern
        The value to check
    """
    if isinstance(input, Pattern):
        return True  # Obviously, it's a pattern
    if isinstance(input, Collection) and not isinstance(input, str):
        return any(_is_pattern(item) for item in input)  # Recurse into the collection
    wildcard_chars = {"*", "?", "$", "^"}
    try:
        value_ = input
        for char in wildcard_chars:
            value_ = value_.replace(rf"\{char}", "")
        return any(char in value_ for char in wildcard_chars)
    except (TypeError, AttributeError):
        return False


def search(
    df: pd.DataFrame,
    query: dict[str, Any],
    columns_with_iterables: Collection[str],
    name_column: str,
    require_all: bool = False,
) -> pd.DataFrame:
    """
    Search for entries in the catalog.

    Parameters
    ----------
    df: :py:class:`~pandas.DataFrame`
        A dataframe to search
    query: dict
        A dictionary of query parameters to execute against the dataframe
    columns_with_iterables: list
        Columns in the dataframe that have iterables
    name_column: str
        The name column in the dataframe catalog
    require_all: bool
        If True, groupby name_column and return only entries that match
        for all elements in each group

    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
            A new dataframe with the entries satisfying the query criteria.

    @TODO: Cleanup & refactoring needed.
    """

    if not query:
        return df

    if isinstance(columns_with_iterables, str):
        columns_with_iterables = [columns_with_iterables]

    lf: pl.LazyFrame = pl.from_pandas(df).lazy()
    all_cols = lf.collect_schema().names()

    # Keep the iterable columns and their dtypes hanging around for later
    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }

    for colname, dtype in iterable_dtypes.items():
        if dtype == ndarray:
            iterable_dtypes[colname] = tuple

    columns_with_iterables = set(columns_with_iterables)
    iterable_qcols = columns_with_iterables.intersection(query)
    cols_to_deiter = set(all_cols).difference(columns_with_iterables, {name_column})

    if require_all and not iterable_qcols:
        # If we've specified require all but we don't have any iterable columns
        # in the query we promote the query columns to iterables at this point.
        (
            lf,
            iterable_qcols_tmp,
            columns_with_iterables,
            cols_to_deiter,
        ) = _promote_query_qcols(
            lf, query, columns_with_iterables, all_cols, cols_to_deiter
        )
    else:
        iterable_qcols_tmp = iterable_qcols.copy()

    lf = lf.with_row_index()
    for column in columns_with_iterables:
        # N.B: Cannot explode multiple columns together as we need a cartesian product
        lf = lf.explode(column)

    lf, tmp_cols = _match_and_filter(lf, query)
    lf = _group_and_filter_on_index(lf, name_column, all_cols, tmp_cols)

    if require_all and iterable_qcols_tmp:
        _agg_cols = set(all_cols).union(set(tmp_cols)) - {name_column}
        lf = _filter_iter_qcols_on_name(
            lf,
            query,
            name_column,
            _agg_cols,
            iterable_qcols_tmp,
            iterable_qcols,
        )

    lf = lf.select(*all_cols)

    df = lf.explode(list(cols_to_deiter)).collect().to_pandas()

    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df


def _group_and_filter_on_index(
    lf: pl.LazyFrame,
    name_column: str,
    all_cols: list[str],
    tmp_cols: list[str],
    /,
) -> pl.LazyFrame:
    return (
        lf.group_by("index")  # Piece the exploded columns back together
        .agg(
            [  # Re-aggregate the exploded columns into lists, flatten them out (imploding creates nested lists) and drop duplicates and nulls
                pl.col(col).flatten().unique(maintain_order=True).drop_nulls()
                for col in [*all_cols, *tmp_cols]
            ]
        )
        .drop("index")  # We don't need the index anymore
        .explode(name_column)  # Explode the name column back out so we can select on it
    )


def _promote_query_qcols(
    lf: pl.LazyFrame,
    query: dict[str, Any],
    columns_with_iterables: set[str],
    all_cols: Collection[str],
    cols_to_deiter: set[str],
    /,
) -> tuple[pl.LazyFrame, set[str], set[str], set[str]]:
    """
    Promote query columns to iterable columns in the lazyframe. Positional-only
    arguments - internal use only.

    """
    iterable_qcols = set(query).intersection(all_cols)

    lf = lf.with_columns(
        [pl.col(colname).cast(pl.List(pl.Utf8)) for colname in iterable_qcols]
    )
    _columns_with_iterables = columns_with_iterables.copy()
    _cols_to_deiter = cols_to_deiter.copy()
    # Keep track of the newly promoted columns & the need to de-iterable them later
    _columns_with_iterables.update(iterable_qcols)
    _cols_to_deiter.update(iterable_qcols)

    return lf, iterable_qcols, _columns_with_iterables, _cols_to_deiter


def _match_and_filter(
    lf: pl.LazyFrame, query: dict[str, Any], /
) -> tuple[pl.LazyFrame, list[str]]:
    """
    Take a lazyframe and a query dict, and add match columns and filter the lazyframe
    accordingly. Positional-only arguments - internal use only.

    Note here we put our *query* into the match column, as we need to match on
    all *queries*, not all *matches*.
    """
    schema = lf.collect_schema()

    for colname, subquery in query.items():
        if schema[colname] == pl.Utf8 and _is_pattern(subquery):
            subquery = [q if _is_pattern(q) else f"^{q}$" for q in subquery]
            # Build match expressions
            match_exprs = [
                pl.when(pl.col(colname).str.contains(q)).then(pl.lit(q)).otherwise(None)
                for q in subquery
            ]
        else:
            # Can't unify these branches with literal=True, because that assumes
            # non-pattern columns *must be strings*, which is not the case.
            match_exprs = [
                pl.when(pl.col(colname) == q).then(pl.lit(q)).otherwise(None)
                for q in subquery
            ]

        _matchlist = pl.concat_list(match_exprs)

        lf = lf.with_columns(
            pl.when(_matchlist.list.drop_nulls().list.len() > 0)
            .then(_matchlist)
            .otherwise(
                None
            )  # This whole when-then-otherwise is to map empty lists to null
            .alias(f"{colname}_matches")
        )

        lf = lf.filter(pl.col(f"{colname}_matches").is_not_null())

    tmp_cols = [f"{colname}_matches" for colname in query.keys()]
    return lf, tmp_cols


def _filter_iter_qcols_on_name(
    lf: pl.LazyFrame,
    query: dict[str, Any],
    name_column: str,
    agg_cols: set[str],
    iterable_qcols_tmp: set[str],
    iterable_qcols: set[str],
    /,
) -> pl.LazyFrame:
    """
    Only ever called if require_all is True.

    Takes a lazyframe and filters it to only those names that match all
    elements in the query for the specified iterable query columns. Positional
    -only arguments - internal use only.

    TODO: I think we need to do the counting *before * we* aggregate, otherwise
    we might count matches that are in different rows as being together. Need to test this.
    """
    group_on_names = not iterable_qcols

    if group_on_names:
        # if True:
        # Group by name_column and aggregate the other columns into lists
        # first in this instance. Essentially the opposite of the previous
        # group_by("index") operation.
        namelist_lf = lf.group_by(name_column).agg(
            [
                pl.col(col).explode().flatten().unique(maintain_order=True)
                for col in agg_cols
            ]
        )
    else:
        namelist_lf = lf

    namelist = (
        namelist_lf.filter(
            [
                pl.col(f"{colname}_matches").list.drop_nulls().list.len()
                >= len(query[colname])
                for colname in iterable_qcols_tmp
            ]
        )
        .select(name_column)
        .collect()
        .to_series()
    )
    return lf.filter(pl.col(name_column).is_in(namelist))
