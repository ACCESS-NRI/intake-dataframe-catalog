# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Stolen and adapted from https://github.com/intake/intake-esm/blob/main/intake_esm/_search.py

import re
import typing

import numpy as np
import pandas as pd


def is_pattern(value: typing.Union[str, typing.Pattern]) -> bool:
    """
    Check whether the passed value is a pattern

    Parameters
    ----------
    value: str or Pattern
        The value to check
    """
    if isinstance(value, typing.Pattern):
        return True
    wildcard_chars = {"*", "?", "$", "^"}
    try:
        value_ = value
        for char in wildcard_chars:
            value_ = value_.replace(rf"\{char}", "")
        return any(char in value_ for char in wildcard_chars)
    except (TypeError, AttributeError):
        return False


def _match_iterables(strings, pattern, regex):
    """
    Given an iterable of strings, return all that match the provided pattern
    as a list.
    """
    matches = []
    for string in strings:
        if regex:
            match = re.match(pattern, string, flags=0)
        else:
            match = pattern == string
        if match:
            matches.append(string)
    return matches


def search(
    df: pd.DataFrame,
    query: dict[str, typing.Any],
    columns_with_iterables: list,
    require_all_on: str = None,
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
    require_all_on: str or None
        If not None, groupby this column and return only entries that match
        for all elements in each group
    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
            A new dataframe with the entries satisfying the query criteria.
    """
    df = df.copy()
    if not query:
        return pd.DataFrame(columns=df.columns)
    global_mask = np.ones(len(df), dtype=bool)
    has_all_mask = np.ones(len(df), dtype=bool)
    for column, values in query.items():
        local_mask = np.zeros(len(df), dtype=bool)
        column_is_stringtype = isinstance(
            df[column].dtype, (object, pd.core.arrays.string_.StringDtype)
        )
        column_has_iterables = column in columns_with_iterables

        if column_has_iterables:
            matched_iterables = pd.Series(np.empty((len(df), 0)).tolist(), name=column)

        for value in values:
            if column_has_iterables:
                matched = df[column].apply(
                    lambda iters: _match_iterables(iters, value, is_pattern(value))
                )
                mask = matched.astype(bool)
                matched_iterables += matched
            elif column_is_stringtype and is_pattern(value):
                mask = df[column].str.contains(value, regex=True, case=True, flags=0)
            else:
                mask = df[column] == value
            local_mask = local_mask + mask.astype(int)

        if require_all_on:
            mask = local_mask.groupby(df[require_all_on]).transform("sum") >= len(
                values
            )
            has_all_mask = has_all_mask & mask

        local_mask = local_mask.astype(bool)

        if column_has_iterables:
            # Overwrite iterable entries with subset found
            cast_type = type(df[column].iloc[0])
            df.loc[
                matched_iterables.loc[local_mask].index, column
            ] = matched_iterables.loc[local_mask].apply(cast_type)

        global_mask = global_mask & local_mask

    if require_all_on:
        global_mask = global_mask & has_all_mask

    results = df.loc[global_mask]
    return results.reset_index(drop=True)
