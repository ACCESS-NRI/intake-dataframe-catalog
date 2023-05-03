# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import ast
import tlz
import typing
import warnings
from io import UnsupportedOperation

import yaml
import fsspec
import numpy as np
import pandas as pd

import intake
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__
from ._search import search

pd.set_option("display.max_colwidth", 200)
pd.set_option("display.max_rows", 8)


class DfFileCatalogError(Exception):
    pass


class DfFileCatalog(Catalog):
    """
    A table of intake (sub)catalogs and associated metadata.
    """

    version = __version__
    container = "catalog"
    partition_access = None
    name = "dataframe_file_cat"

    def __init__(
        self,
        path: str = None,
        yaml_column: str = "yaml",
        name_column: str = "name",
        mode: str = "r",
        columns_with_iterables: list[str] = None,
        storage_options: dict[str, typing.Any] = None,
        read_kwargs: dict[str, typing.Any] = None,
        **intake_kwargs: dict[str, typing.Any],
    ):
        """
        Parameters
        ----------
        path : str
            Path to the DF catalog file.
        yaml_column: str, optional
            Name of the column in the tabular file containing intake yaml descriptions of the intake
            (sub)catalogs.
        name_column: str, optional
            Name of the column in the tabular file containing the names of the intake (sub)catalogs.
        mode: str, optional
            The access mode. Options are:
            - `r` for read-only; no data can be modified.
            - `w` for write; a new file is created, an existing file with the same name is deleted.
            - `x` for write, but fail if an existing file with the same name already exists.
            - `a` and `r+` for update; an existing file is opened for reading and writing.
        columns_with_iterables : list of str, optional
            A list of columns in the tabular file containing iterables. Values in columns specified here will be
            converted with `ast.literal_eval` when :py:func:`~pandas.read_csv` is called (i.e., this is a
            shortcut to passing converters to `read_kwargs`).
        storage_options : dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        read_kwargs : dict, optional
            Additional keyword arguments passed to :py:func:`~pandas.read_csv` when reading from
            the DFFileCatalog.
        intake_kwargs : dict, optional
            Additional keyword arguments to pass to the :py:class:`~intake.catalog.Catalog` base class.
        """

        self.path = path
        self.yaml_column = yaml_column
        self.name_column = name_column
        self.mode = mode
        self._columns_with_iterables = columns_with_iterables
        self.storage_options = storage_options or {}
        self._intake_kwargs = intake_kwargs or {}

        read_kwargs = read_kwargs.copy() if read_kwargs else {}
        if self._columns_with_iterables:
            converter = ast.literal_eval
            read_kwargs.setdefault("converters", {})
            for col in self._columns_with_iterables:
                if read_kwargs["converters"].setdefault(col, converter) != converter:
                    raise ValueError(
                        f"Cannot provide converter for '{col}' via `read_kwargs` when '{col}' is also specified "
                        "in `columns_with_iterables`"
                    )
        self._read_kwargs = read_kwargs

        self._entries = {}
        self._df = pd.DataFrame(columns=[self.name_column, self.yaml_column])
        self._df_summary = None

        self._allow_write = False
        self._try_overwrite = False
        if self.mode in ["w", "x"]:
            self._try_overwrite = True
        if self.mode in ["w", "x", "a", "r+"]:
            self._allow_write = True

        super().__init__(storage_options=self.storage_options, **self._intake_kwargs)

    def _load(self) -> None:
        """
        Load the DF catalog from file.
        """
        if self.path:
            if self._try_overwrite:
                with fsspec.open(
                    self.path, mode=self.mode, **self.storage_options
                ) as fobj:
                    pass
                    # self._df.to_csv(fobj)
            else:
                with fsspec.open(self.path, **self.storage_options) as fobj:
                    self._df = pd.read_csv(fobj, **self._read_kwargs)
                if self.yaml_column not in self.df.columns:
                    raise DfFileCatalogError(
                        f"'{self.yaml_column}' is not a column in the DF catalog. Please provide "
                        "the name of the column containing intake YAML descriptions via argument "
                        "`yaml_column`."
                    )
                if self.name_column not in self.df.columns:
                    raise DfFileCatalogError(
                        f"'{self.name_column}' is not a column in the DF catalog. Please provide "
                        "the name of the column containing subcatalog names via argument "
                        "`name_column`."
                    )

    def __len__(self) -> int:
        return len(self.keys())

    def __contains__(self, key: str) -> bool:
        # Base Catalog class loads all entries via _get_entries, so implement it differently
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __getitem__(self, key: str) -> intake.DataSource:
        try:
            return self._entries[key]
        except KeyError as e:
            if key in self.keys():
                yamls = list(
                    self.df.loc[self.df[self.name_column] == key, self.yaml_column]
                )
                yaml_text = yamls[0]
                # If there are multiple entries with the same name, make sure they all point to
                # the same catalog
                if len(yamls) > 1:
                    assert all(y == yaml_text for y in yamls)

                self._entries[key] = LocalCatalogEntry(
                    name=key, **yaml.safe_load(yaml_text)["sources"][key]
                ).get()
                return self._entries[key]
            raise KeyError(
                f"key='{key}' not found in catalog. You can access the list of valid keys via the .keys() method."
            ) from e

    def __repr__(self) -> str:
        return (
            f"<{self.name or 'Intake-dataframe'} catalog with {len(self)} subcatalog(s) across "
            f"{len(self.df)} rows>"
        )

    def _repr_html_(self) -> str:
        """
        Return an html summary for the DF catalog object. Mainly for IPython notebook
        """

        text = self.df_summary._repr_html_()

        return (
            f"<p><strong>{self.name or 'Intake-dataframe'} catalog with {len(self)} subcatalog(s) across "
            f"{len(self.df)} rows</strong>:</p> {text}"
        )

    def _ipython_display_(self):
        """
        Display the entry as a rich object in an IPython session
        """
        from IPython.display import HTML, display

        contents = self._repr_html_()
        display(HTML(contents))

    def keys(self) -> list[str]:
        """
        Return a list of keys for the DF catalog entries.
        """
        return self.unique()[self.name_column]

    def _get_entries(self) -> dict[str, intake.DataSource]:
        """
        Make sure all entries are in self._entries. Entries are created just-in-time so we need to make sure to
        create entries missing from self._entries.

        Returns
        -------
        entries: dict
            Dictionary of all available entries in the DF catalog.
        """

        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]

        return self._entries

    def _unique(self) -> dict:
        def _find_unique(series):
            values = series.dropna()
            if series.name in self.columns_with_iterables:
                values = tlz.concat(values)
            return list(tlz.unique(values))

        if self._df.empty:
            return {col: [] for col in self.columns}
        else:
            return self.df.apply(_find_unique, result_type="reduce").to_dict()

    def unique(self) -> pd.Series:
        """
        Return a series of unique values for each column in the DF catalog, excluding the
        yaml description column.
        """
        return pd.Series(self._unique()).drop(self.yaml_column)

    def nunique(self) -> pd.Series:
        """
        Return a series of the number of unique values for each column in the DF catalog,
        excluding the yaml description column.
        """
        return pd.Series(tlz.valmap(len, self._unique())).drop(self.yaml_column)

    def add(
        self,
        cat: intake.DataSource,
        metadata: dict[str, typing.Any] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Add an intake (sub)catalog to the DF catalog.

        Parameters
        ----------
        cat: object
            An intake DataSource object (or child thereof) with a .yaml() method.
        metadata : dict, optional
            Dictionary of metadata associated with the intake (sub)catalog. If an entry is provided for
            the 'name_column', the catalog name will be overwritten with this value. Otherwise the
            'name_column' entry is taken from the intake catalog name if it exists, failing otherwise.
        overwrite : bool, optional
            If True, overwrite all existing entries in the DF catalog with name_column entries that
            match the name of this cat.
        """

        metadata = metadata or {}
        metadata_keys = list(metadata.keys())

        if self.name_column in metadata:
            cat.name = metadata[self.name_column]
        else:
            if cat.name:
                metadata[self.name_column] = cat.name
            else:
                raise DfFileCatalogError(
                    "Cannot add an unnamed catalog to the DF catalog. Either set the name attribute on "
                    "the catalog being add or provide an entry in the input argument 'metadata' "
                    "corresponding to the 'name_column' of the DF catalog"
                )
        metadata[self.yaml_column] = cat.yaml()

        row = pd.DataFrame({k: 0 for k in metadata.keys()}, index=[0])
        row.iloc[0] = pd.Series(metadata)

        if self._df.empty:
            self._df = row
        else:
            # Check that new entries contain iterables when they should
            entry_iterable_columns = _columns_with_iterables(row)
            if entry_iterable_columns != self.columns_with_iterables:
                raise DfFileCatalogError(
                    f"Cannot add entry with iterable metadata columns: {entry_iterable_columns} "
                    f"to DF catalog with iterable metadata columns: {self.columns_with_iterables}. "
                    " Please ensure that metadata entries are consistent."
                )

            if set(self.columns) == set(row.columns):
                if (
                    metadata[self.name_column] in self.df[self.name_column].unique()
                ) and overwrite:
                    self.remove(entry=metadata[self.name_column])

                self._df = pd.concat([self._df, row], ignore_index=True)
            else:
                metadata_columns = self.columns
                metadata_columns.remove(self.name_column)
                metadata_columns.remove(self.yaml_column)
                raise DfFileCatalogError(
                    f"metadata must include the following keys to be added to this DF catalog: {metadata_columns}. "
                    f"You passed a dictionary with the following keys: {metadata_keys}"
                )

        # Force recompute df_summary
        self._df_summary = None

    def remove(self, entry: str) -> None:
        """
        Remove an intake (sub)catalog from the DF catalog.

        Parameters
        ----------
        entry: str
            The corresponding 'name_column' entry for the (sub)catalog to remove.
        """

        if entry in self.df[self.name_column].unique():
            self._df.drop(
                self._df[self._df[self.name_column] == entry].index,
                inplace=True,
            )
        else:
            raise ValueError(
                f"'{entry}' is not an entry in the '{self.name_column}' column of the DF catalog"
            )

        # Drop metadata columns if there are no entries
        if self._df.empty:
            self._df = self._df[[self.name_column, self.yaml_column]]

        # Force recompute df_summary
        self._df_summary = None

    def search(self, require_all: bool = False, **query: typing.Any) -> pd.DataFrame:
        """
        Search for subcatalogs in the DF catalog. Multiple columns can be queried simutaneously by
        passing multiple queries. Only subcatalogs that satisfy all column queries are returned.
        Additionally, multiple values within a column can be queried by passing a list of values to
        query on. By default, a column query is considered to match if any of the values are found
        in the corresponding subcatalog metadata (see the `require_all` argument).

        Parameters
        ----------
        query: dict, optional
            A dictionary of query parameters to execute against the DF catalog: {column_name: value[s]}.
        require_all : bool, optional
            If True, returned subcatalogs satisfy all the query criteria. For example, a query of
            `variable = ["a", "b"]` with `require_all = True` will return only subcatalogs that
            contain _both_ variables "a" and "b".

        Returns
        -------
        dataframe: :py:class:`~pandas.DataFrame`
            A new dataframe with the entries satisfying the query criteria.
        """
        columns = self.columns

        for key, value in query.items():
            if key not in columns:
                raise ValueError(f"Column '{key}' not in columns {columns}")
            if not isinstance(query[key], list):
                query[key] = [query[key]]

        results = search(
            df=self.df, query=query, columns_with_iterables=self.columns_with_iterables
        )

        mode = "a" if self.mode in ["w", "x"] else self.mode

        cat = self.__class__(
            path=self.path,
            yaml_column=self.yaml_column,
            name_column=self.name_column,
            mode=mode,
            columns_with_iterables=self.columns_with_iterables,
            storage_options=self.storage_options,
            read_kwargs=self._read_kwargs,
            **self._intake_kwargs,
        )
        cat._df = results

        if require_all and not cat._df.empty:
            # Remove any entries that do not satisfy all queries
            mask = np.zeros(len(cat.df_summary), dtype=bool)
            for column, values in query.items():
                for value in values:
                    not_in = ~cat.df_summary[column].str.contains(value, regex=False)
                    mask = mask | not_in

            for remove in cat.df_summary.index[mask].tolist():
                cat.remove(remove)

        return cat

    def save(
        self,
        path: str = None,
        **kwargs: dict[str, typing.Any],
    ) -> None:
        """
        Save a DF catalog to a file.

        Parameters
        ----------
        path : str or None, optional
            Location to save the catalog. If None, the path specified at initialisation
            of the catalog is used.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~pandas.DataFrame.to_csv`.
        """

        save_path = path if path else self.path

        if self._allow_write:
            mapper = fsspec.get_mapper(
                f"{save_path}", storage_options=self.storage_options
            )
            fs = mapper.fs
            fname = f"{mapper.fs.protocol}://{save_path}"

            csv_kwargs = {"index": False}
            csv_kwargs.update(kwargs.copy() or {})

            with fs.open(fname, "wb") as fobj:
                self.df.to_csv(fobj, **csv_kwargs)
        else:
            raise UnsupportedOperation(
                f"Cannot save catalog initialised with mode='{self.mode}'"
            )

    serialize = save

    def to_subcatalog_dict(
        self, **kwargs: dict[str, typing.Any]
    ) -> dict[str, typing.Any]:
        """
        Load DF catalog entries into a dictionary of intake subcatalogs.

        Parameters
        ----------
        kwargs: dict
            Arguments/user parameters to use for opening the subcatalog(s). For example, many intake drivers support
            a `storage_options` argument with parameters to be passed to the backend file-system. Note, this function
            passes the same kwargs to all subcatalogs in the DF catalog. To pass different kwargs to different
            subcatalogs, load each subcatalog using it's key (name), e.g. `dfcat["<subcat_name>"](**kwargs)`.

        Returns
        -------
        subcatalogs: dict
            A dictionary of subcatalogs.
        """

        if not self.keys():
            warnings.warn(
                "There are no subcatalogs to open. Returning an empty dictionary.",
                UserWarning,
                stacklevel=2,
            )

        return {key: subcat(**kwargs) for key, subcat in self.items()}

    def to_subcatalog(self, **kwargs: dict[str, typing.Any]) -> intake.DataSource:
        """
        Load intake subcatalog. This is only possible if there is only one remaining subcatalog in the DF catalog.

        Parameters
        ----------
        kwargs: dict
            Arguments/user parameters to use for opening the subcatalog. For example, many intake drivers support
            a `storage_options` argument with parameters to be passed to the backend file-system.`.

        Returns
        -------
        subcatalog: :py:class:`intake.DataSource`
            A dictionary of subcatalogs.
        """

        if len(self) == 1:
            res = self.to_subcatalog_dict(**kwargs)
            _, subcat = res.popitem()
            return subcat
        else:
            raise ValueError(
                f"Expected exactly one subcatalog, received {len(self)}. Please refine your search or use "
                "`.to_subcatalog_dict()`."
            )

    @property
    def df(self) -> pd.DataFrame:
        """
        Return a :py:class:`~pandas.DataFrame` representation of the DF catalog. This property is mostly for
        internal use. Users may find the `df_summary` property more useful.
        """
        return self._df

    @property
    def columns(self) -> list[str]:
        """
        Return a list of the columns in the DF catalog.
        """
        return self.df.columns.tolist()

    @property
    def columns_with_iterables(self) -> set[str]:
        """
        Return a list of the columns in the DF catalog that have iterables.
        """

        if self._columns_with_iterables is None:
            if self._df.empty:
                return list()

            self._columns_with_iterables = _columns_with_iterables(
                self._df, sample=True
            )

        return self._columns_with_iterables

    @property
    def df_summary(self) -> pd.DataFrame:
        """
        Return a :py:class:`~pandas.DataFrame` summary of unique entries in DF catalog.
        """

        def _list_unique(series):
            uniques = set(
                series.apply(
                    lambda x: list(x)
                    if series.name in self.columns_with_iterables
                    else [
                        x,
                    ]
                ).sum()
            )
            return uniques  # uniques[0] if len(uniques) == 1 else uniques

        if self._df.empty:
            self._df_summary = self.df.set_index(self.name_column).drop(
                columns=self.yaml_column
            )
        elif self._df_summary is None:
            self._df_summary = self.df.groupby(self.name_column).agg(
                {
                    col: _list_unique
                    for col in self.df.columns.drop(
                        [self.name_column, self.yaml_column]
                    )
                }
            )

        return self._df_summary


def _columns_with_iterables(df, sample=False):
    """
    Return a list of the columns in the provided pandas dataframe/series that have iterables.
    Stolen from https://github.com/intake/intake-esm/blob/main/intake_esm/cat.py#L277
    """

    _df = df.sample(20, replace=True) if sample else df

    has_iterables = _df.applymap(type).isin([list, tuple, set]).any().to_dict()

    return [col for col, check in has_iterables.items() if check]
