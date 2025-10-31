# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Stolen and adapted from https://github.com/intake/intake-esm/blob/main/intake_esm/_search.py

from collections.abc import Collection
from re import Pattern
from typing import Any, Union

import pandas as pd
import polars as pl


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
    if require_all and len(query.get(name_column, [""])) > 1:
        return df.head(0)

    if isinstance(columns_with_iterables, str):
        columns_with_iterables = [columns_with_iterables]

    lf: pl.LazyFrame = pl.from_pandas(df).lazy()
    all_cols = lf.collect_schema().names()

    # Keep the iterable columns and their dtypes hanging around for later
    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }
    columns_with_iterables = set(columns_with_iterables)
    iterable_qcols = columns_with_iterables.intersection(query)
    cols_to_deiter = set(all_cols).difference(columns_with_iterables, {name_column})

    if require_all and not iterable_qcols:
        # If we've specified require all but we don't have any iterable columns
        # in the query we promote the query columns to iterables at this point.
        group_on_names = True
        iterable_qcols = set(query).intersection(all_cols)

        lf = lf.with_columns(
            [pl.col(colname).cast(pl.List(pl.Utf8)) for colname in iterable_qcols]
        )
        # Keep track of the newly promoted columns & the need to de-iterable them later
        columns_with_iterables.update(iterable_qcols)
        cols_to_deiter.update(iterable_qcols)
    else:
        group_on_names = False

    lf = lf.with_row_index()
    for column in columns_with_iterables:
        lf = lf.explode(column)

    tmp_cols = []
    for colname, subquery in query.items():
        if lf.collect_schema()[colname] == pl.Utf8 and _is_pattern(subquery):
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

        lf = lf.with_columns(
            pl.when(pl.concat_list(match_exprs).list.drop_nulls().list.len() > 0)
            .then(pl.concat_list(match_exprs))
            .otherwise(
                None
            )  # This whole when-then-otherwise is to map empty lists to null
            .alias(f"{colname}_matches")
        )

        lf = lf.filter(pl.col(f"{colname}_matches").is_not_null())
        tmp_cols.append(f"{colname}_matches")

    lf = (
        lf.group_by("index")  # Piece the exploded columns back together
        .agg(
            [  # Re-aggregate the exploded columns into lists, flatten them out (imploding creates nested lists) and drop duplicates and nulls
                pl.col(col).implode().flatten().unique(maintain_order=True).drop_nulls()
                for col in [*all_cols, *tmp_cols]
            ]
        )
        .drop("index")  # We don't need the index anymore
        .explode(name_column)  # Explode the name column back out so we can select on it
    )

    if require_all and iterable_qcols:
        if group_on_names:
            # Group by name_column and aggregate the other columns into lists
            # first in this instance. Essentially the opposite of the previous
            # group_by("index") operation.
            namelist_lf = lf.group_by(name_column).agg(
                [
                    pl.col(col).explode().flatten().unique(maintain_order=True)
                    for col in (set(all_cols).union(set(tmp_cols)) - {name_column})
                ]
            )
        else:
            namelist_lf = lf

        namelist = (
            namelist_lf.filter(
                [
                    pl.col(f"{colname}_matches").list.drop_nulls().list.len()
                    >= len(query[colname])
                    for colname in iterable_qcols
                ]
            )
            .select(name_column)
            .collect()
            .to_series()
        )
        lf = lf.filter(pl.col(name_column).is_in(namelist))

    lf = lf.select(*all_cols)

    df = lf.explode(list(cols_to_deiter)).collect().to_pandas()

    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df
