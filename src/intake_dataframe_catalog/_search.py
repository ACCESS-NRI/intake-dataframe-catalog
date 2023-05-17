# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Stolen and adapted from https://github.com/intake/intake-esm/blob/main/intake_esm/_search.py

import itertools
import re
import typing

import numpy as np
import pandas as pd
import tlz


def _is_pattern(value: typing.Union[str, typing.Pattern]) -> bool:
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


def _match_iterables(
    strings: typing.Union[list, tuple, set], pattern: str, regex: bool
):
    """
    Given an iterable of strings, return all that match the provided pattern.
    """
    matches = []
    for string in strings:
        if regex:
            match = re.match(pattern, string, flags=0)
        else:
            match = pattern == string
        if match:
            matches.append(string)
    return type(strings)(matches)


def search(
    df: pd.DataFrame,
    query: dict[str, typing.Any],
    columns_with_iterables: list,
    name_column: str,
    require_all: str = False,
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
    require_all: str or None
        If True, groupby name_column and return only entries that match
        for all elements in each group
    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
            A new dataframe with the entries satisfying the query criteria.
    """

    df = df.copy()

    if not query:
        return df

    # 1. First create a mask for each query
    searches = [(key, val) for key, vals in query.items() for val in vals]
    search_matches = {column: {} for column in query.keys()}

    matched_iterables = pd.DataFrame()

    for (column, value) in searches:

        is_pattern = _is_pattern(value)

        if column in columns_with_iterables:
            matches = df[column].apply(
                lambda values: _match_iterables(values, value, is_pattern)
            )
            match = matches.astype(bool)

            # Keep track of which iterables matched
            if column in matched_iterables.columns:
                matched_iterables[column] = matched_iterables[column].combine(
                    matches, func=lambda s1, s2: type(s1)(tlz.concat([s1, s2]))
                )
            else:
                matched_iterables[column] = matches

        elif is_pattern:
            match = df[column].str.contains(value, regex=True, case=True, flags=0)
        else:
            match = df[column] == value

        search_matches[column][value] = match

    # 2. Now combine the masks
    conditions = set(itertools.product(*[tuple(v) for v in query.values()]))

    groups = df[name_column]

    global_match = np.zeros(len(df), dtype=bool)
    n_conditions_in_group = np.zeros(groups.nunique(), dtype=bool)

    for condition in conditions:

        condition_match = np.ones(len(df), dtype=bool)

        for column, value in zip(query.keys(), condition):

            condition_match = condition_match & search_matches[column][value]

        n_conditions_in_group = n_conditions_in_group + condition_match.groupby(
            groups
        ).any().astype(int)

        global_match = global_match | condition_match

    # 3. Replace queried columns with iterables with reduced versions
    if not matched_iterables.empty:
        df[matched_iterables.columns] = matched_iterables

    if require_all:
        has_all = n_conditions_in_group == len(conditions)
        # Expand has_all_mask across all groups
        has_all = has_all.loc[groups].reset_index(drop=True)
        global_match = global_match & has_all
    else:
        global_match = global_match

    return df.loc[global_match]
