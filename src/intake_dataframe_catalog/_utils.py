from collections import defaultdict

import polars as pl


class MinimalExploder:
    """
    A comprehensive class for analyzing and performing minimal explosions
    of DataFrames with multiple list columns.
    """

    def __init__(self, df: pl.DataFrame):
        self.df = df
        self._list_cols: list[str] | None = None
        self._length_patterns: dict[str, tuple[int, ...]] | None = None
        self._explodable_groups: list[list[str]] | None = None

    @property
    def list_columns(self) -> list[str]:
        """Get all list-type columns in the DataFrame."""
        if self._list_cols is None:
            self._list_cols = [
                col for col in self.df.columns if self.df[col].dtype == pl.List
            ]
        return self._list_cols

    @property
    def length_patterns(self) -> dict[str, tuple[int, ...]]:
        """Get length patterns for all list columns.

        This is stored as a dictionary containing tuples of all list lengths, ie
        'a' : (1,3,2),
        'b' : (2,2,2),

        """
        if self._length_patterns is None:
            self._length_patterns = self._analyze_patterns()
        return self._length_patterns

    @property
    def explodable_groups(self) -> list[list[str]]:
        """Get groups of columns that can be exploded together."""
        if self._explodable_groups is None:
            self._explodable_groups = self._compute_groups()
        return self._explodable_groups

    def _analyze_patterns(self) -> dict[str, tuple[int, ...]]:
        """Analyze length patterns of all list columns. Returns a value
        rather than setting self._length_patterns to shut up mypy."""
        _length_patterns = {}

        for col in self.list_columns:
            lengths = self.df.select(pl.col(col).list.len()).to_series().to_list()
            _length_patterns[col] = tuple(lengths)

        return _length_patterns

    def _compute_groups(self):
        """Compute explodable groups based on length patterns. Returns a value
        rather than setting self._explodable_groups to shut up mypy."""
        pattern_groups = defaultdict(list)

        for col, pattern in self.length_patterns.items():
            pattern_groups[pattern].append(col)

        return list(pattern_groups.values())

    @property
    def summary(self) -> dict:
        """Get a summary of the explosion analysis."""
        return {
            "total_columns": len(self.df.columns),
            "list_columns": len(self.list_columns),
            "unique_patterns": len(set(self.length_patterns.values())),
            "explodable_groups": len(self.explodable_groups),
            "explosion_operations_needed": len(self.explodable_groups),
            "groups": self.explodable_groups,
        }

    def __call__(self) -> pl.DataFrame:
        """Perform the minimal explosion."""
        if not self.list_columns:
            return self.df

        result_df = self.df
        for group in self.explodable_groups:
            result_df = result_df.explode(*group)

        return result_df
