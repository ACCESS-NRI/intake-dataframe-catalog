# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import ast
import typing
import warnings
from io import UnsupportedOperation

import fsspec
import intake
import pandas as pd
import tlz
import yaml
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__
from ._search import search

pd.set_option("display.max_colwidth", 200)
pd.set_option("display.max_rows", None)


class DfFileCatalogError(Exception):
    pass


class DfFileCatalog(Catalog):
    """
    A table of intake sources and associated metadata.
    """

    version = __version__
    container = "catalog"
    partition_access = None
    name = "dataframe_catalog"

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
        Initialise a DfFileCatalog.

        Parameters
        ----------
        path: str
            Path to the dataframe catalog file.
        yaml_column: str, optional
            Name of the column in the dataframe catalog file containing intake yaml descriptions of the
            intake sources.
        name_column: str, optional
            Name of the column in the dataframe catalog file containing the names of the intake sources.
        mode: str, optional
            The access mode. Options are:
            - `r` for read-only; no data can be modified.
            - `w` for write; a new file is created, an existing file with the same name is deleted.
            - `x` for write, but fail if an existing file with the same name already exists.
            - `a` and `r+` for update; an existing file is opened for reading and writing.
        columns_with_iterables: list of str, optional
            A list of columns in the dataframe catalog file containing iterables. Values in columns specified
            here will be converted with `ast.literal_eval` when pandas :py:func:`~pandas.read_csv` is called
            (i.e., this is a shortcut to passing converters to `read_kwargs`).
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        read_kwargs: dict, optional
            Additional keyword arguments passed to pands :py:func:`~pandas.read_csv` when reading from
            the DFFileCatalog.
        intake_kwargs: dict, optional
            Additional keyword arguments to pass to the intake :py:class:`~intake.catalog.Catalog` base class.
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
                        "in `columns_with_iterables`."
                    )
        self._read_kwargs = read_kwargs

        self._entries = {}
        self._df = pd.DataFrame(columns=[self.name_column, self.yaml_column])
        self._df_summary = None
        self._previous_search_query = None

        self._allow_write = False
        self._try_overwrite = False
        if self.mode in ["w", "x"]:
            self._try_overwrite = True
        if self.mode in ["w", "x", "a", "r+"]:
            self._allow_write = True

        super().__init__(storage_options=self.storage_options, **self._intake_kwargs)

    def _load(self) -> None:
        """
        Load the dataframe catalog from file.
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
                        f"'{self.yaml_column}' is not a column in the dataframe catalog. Please provide "
                        "the name of the column containing the intake source YAML descriptions via "
                        "argument `yaml_column`."
                    )
                if self.name_column not in self.df.columns:
                    raise DfFileCatalogError(
                        f"'{self.name_column}' is not a column in the dataframe catalog. Please provide "
                        "the name of the column containing the intake source names via argument "
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
                f"key='{key}' not found in catalog. You can access the list of valid source keys via the .keys() method."
            ) from e

    def __repr__(self) -> str:
        return (
            f"<{self.name or 'Intake dataframe'} catalog with {len(self)} source(s) across "
            f"{len(self.df)} rows>"
        )

    def _repr_html_(self) -> str:
        """
        Return an html summary for the dataframe catalog object. Mainly for IPython notebook.
        """

        text = f"<div style='max-height: 300px; overflow: auto; width: fit-content'>{self.df_summary._repr_html_()}</div>"

        return (
            f"<p><strong>{self.name or 'Intake dataframe'} catalog with {len(self)} source(s) across "
            f"{len(self.df)} rows</strong>:</p> {text}"
        )

    def _ipython_display_(self):
        """
        Display the dataframe catalog object as a rich object in an IPython session.
        """
        from IPython.display import HTML, display

        contents = self._repr_html_()
        display(HTML(contents))

    def keys(self) -> list[str]:
        """
        Return a list of keys for the dataframe catalog entries (sources).
        """
        return self.unique()[self.name_column]

    def _get_entries(self) -> dict[str, intake.DataSource]:
        """
        Make sure all entries are in self._entries. Entries are created just-in-time so we need to make sure to
        create entries missing from self._entries.

        Returns
        -------
        entries: dict
            Dictionary of all available entries (sources) in the dataframe catalog.
        """

        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]

        return self._entries

    def _unique(self) -> dict:
        if self._df.empty:
            return {col: [] for col in self.columns}
        else:
            return self.df.apply(
                lambda x: list(_find_unique(x, self.columns_with_iterables)),
                result_type="reduce",
            ).to_dict()

    def unique(self) -> pd.Series:
        """
        Return a series of unique values for each column in the dataframe catalog, excluding the
        yaml description column.
        """
        return pd.Series(self._unique()).drop(self.yaml_column)

    def nunique(self) -> pd.Series:
        """
        Return a series of the number of unique values for each column in the dataframe catalog,
        excluding the yaml description column.
        """
        return pd.Series(tlz.valmap(len, self._unique())).drop(self.yaml_column)

    def add(
        self,
        source: intake.DataSource,
        metadata: dict[str, typing.Any] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Add an intake source to the dataframe catalog.

        Parameters
        ----------
        source: object
            An intake source object with a .yaml() method.
        metadata: dict, optional
            Dictionary of metadata associated with the intake source. If an entry is provided
            corresponding to the 'name_column', the source name will be overwritten with this value.
            Otherwise the corresponding 'name_column' entry is taken from the intake source name if
            it exists, failing otherwise.
        overwrite: bool, optional
            If True, overwrite all existing entries in the dataframe catalog with name_column entries
            that match the name of this source. Otherwise the entry is appended to the dataframe catalog.
        """

        metadata = metadata or {}
        metadata_keys = list(metadata.keys())

        if self.name_column in metadata:
            source.name = metadata[self.name_column]
        else:
            if source.name:
                metadata[self.name_column] = source.name
            else:
                raise DfFileCatalogError(
                    "Cannot add an unnamed source to the dataframe catalog. Either set the name attribute "
                    "on the source being add or provide an entry in the input argument 'metadata' "
                    "corresponding to the 'name_column' of the dataframe catalog."
                )
        metadata[self.yaml_column] = source.yaml()

        row = pd.DataFrame({k: "" for k in metadata.keys()}, index=[0])
        row.iloc[0] = pd.Series(metadata)

        if self._df.empty:
            self._df = row
        else:
            # Check that new entries contain iterables when they should
            entry_iterable_columns = _columns_with_iterables(row)
            if entry_iterable_columns != self.columns_with_iterables:
                raise DfFileCatalogError(
                    f"Cannot add entry with iterable metadata columns: {entry_iterable_columns} "
                    f"to dataframe catalog with iterable metadata columns: {self.columns_with_iterables}. "
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
                    "metadata must include the following keys to be added to this dataframe catalog: "
                    f"{metadata_columns}. You passed a dictionary with the following keys: {metadata_keys}."
                )

        # Force recompute df_summary
        self._df_summary = None

    def remove(self, entry: str) -> None:
        """
        Remove an intake source from the dataframe catalog.

        Parameters
        ----------
        entry: str
            The corresponding 'name_column' entry for the source to remove.
        """

        if entry in self.df[self.name_column].unique():
            self._df.drop(
                self._df[self._df[self.name_column] == entry].index,
                inplace=True,
            )
        else:
            raise ValueError(
                f"'{entry}' is not an entry in the '{self.name_column}' column of the dataframe catalog."
            )

        # Drop metadata columns if there are no entries
        if self._df.empty:
            self._df = self._df[[self.name_column, self.yaml_column]]

        # Force recompute df_summary
        self._df_summary = None

    def search(self, require_all: bool = False, **query: typing.Any) -> "DfFileCatalog":
        """
        Search for sources in the dataframe catalog. Multiple columns can be queried simultaneously
        by passing multiple queries. Only sources that satisfy all column queries are returned.
        Additionally, multiple values within a column can be queried by passing a list of values to
        query on. By default, a column query is considered to match if any of the values are found
        in the corresponding source metadata (see the `require_all` argument).

        Parameters
        ----------
        query: dict, optional
            A dictionary of query parameters to execute against the dataframe catalog of the form
            {column_name: value[s]}.
        require_all : bool, optional
            If True, returned sources satisfy all the query criteria. For example, a query of
            `variable = ["a", "b"]` with `require_all = True` will return only sources that
            contain _both_ variables "a" and "b".

        Returns
        -------
        catalog: :py:class:`~intake_dataframe_catalog.core.DfFileCatalog`
            A new dataframe catalog with the entries satisfying the query criteria.
        """
        columns = self.columns

        for key, value in query.items():
            if key not in columns:
                raise ValueError(f"Column '{key}' not in columns {columns}")
            if not isinstance(query[key], list):
                query[key] = [query[key]]

        results = search(
            df=self.df,
            query=query,
            columns_with_iterables=self.columns_with_iterables,
            name_column=self.name_column,
            require_all=require_all,
        )

        cat = self.__class__(
            yaml_column=self.yaml_column,
            name_column=self.name_column,
            mode=self.mode,
            columns_with_iterables=self.columns_with_iterables,
            storage_options=self.storage_options,
            read_kwargs=self._read_kwargs,
            **self._intake_kwargs,
        )
        cat.path = self.path
        cat._df = results
        cat._previous_search_query = query

        return cat

    def save(
        self,
        path: str = None,
        **kwargs: dict[str, typing.Any],
    ) -> None:
        """
        Save the dataframe catalog to a file.

        Parameters
        ----------
        path : str or None, optional
            Location to save the catalog. If None, the path specified at initialisation
            of the catalog is used.
        kwargs: dict, optional
            Additional keyword arguments passed to pandas :py:func:`~pandas.DataFrame.to_csv`.
        """

        save_path = path if path else self.path

        if self._allow_write:
            mapper = fsspec.get_mapper(
                f"{save_path}", storage_options=self.storage_options
            )
            fs = mapper.fs
            fname = fs.unstrip_protocol(save_path)

            csv_kwargs = {"index": False}
            csv_kwargs.update(kwargs.copy() or {})

            with fs.open(fname, "wb") as fobj:
                self.df.to_csv(fobj, **csv_kwargs)
        else:
            raise UnsupportedOperation(
                f"Cannot save catalog initialised with mode='{self.mode}'"
            )

    serialize = save

    def to_source_dict(
        self, pass_query=False, **kwargs: dict[str, typing.Any]
    ) -> dict[str, typing.Any]:
        """
        Load dataframe catalog entries into a dictionary of intake sources.

        Parameters
        ----------
        pass_query: boolean, optional
            If True, blindly pass the most recent query provided to `self.search` on to the `.search` method of the
            source(s). An exception is thrown if the source does not have a `.search` method, or if a method exists
            that does not support dictionary queries of the type provided to `self.search`.
        kwargs: dict
            Arguments/user parameters to use for opening the sources. For example, many intake drivers support
            a `storage_options` argument with parameters to be passed to the backend file-system. Note, this function
            passes the same kwargs to all sources in the dataframe catalog. To pass different kwargs to different
            sources, load each source using it's key (name), e.g. `cat["<source_name>"](**kwargs)`.

        Returns
        -------
        sources: dict
            A dictionary of intake sources.
        """

        if not self.keys():
            warnings.warn(
                "There are no sources to open. Returning an empty dictionary.",
                UserWarning,
                stacklevel=2,
            )

        sources = {key: source(**kwargs) for key, source in self.items()}

        if pass_query:
            if self._previous_search_query:
                sources_searched = {}
                for key, source in sources.items():
                    if not hasattr(source, "search"):
                        raise DfFileCatalogError(
                            f"The source '{key}' ({source.classname}) does not have a `.search` method and so cannot "
                            "be loaded with `pass_query = True`."
                        )
                    else:
                        # Try to pass each query sequentially
                        for col, val in self._previous_search_query.items():
                            try:
                                source = source.search(**{col: val})
                            except TypeError:
                                raise DfFileCatalogError(
                                    f"The source '{key}' ({source.classname}) has a `.search` method with a different "
                                    "API than `self.search` and so cannot be loaded with `pass_query = True`."
                                )
                            except Exception:
                                # Only warn here so that valid queries can still be applied
                                warnings.warn(
                                    f"Unable to pass query on '{col}' on to source '{key}' so this query is being "
                                    f"skipped. This is usually because '{col}' is not a valid query key in the source. "
                                    f"For example, if the source is an intake-esm datastore, the column '{col}' may "
                                    "not exist or may be called something else.",
                                    UserWarning,
                                    stacklevel=2,
                                )

                    sources_searched[key] = source
                sources = sources_searched
            else:
                raise DfFileCatalogError(
                    "No previous queries exist to pass on to source(s)"
                )

        return sources

    def to_source(
        self, pass_query=False, **kwargs: dict[str, typing.Any]
    ) -> intake.DataSource:
        """
        Load intake source. This is only possible if there is only one remaining source in the dataframe
        catalog.

        Parameters
        ----------
        pass_query: boolean, optional
            If True, blindly pass the most recent query provided to `self.search` on to the `.search` method of the
            source. An exception is thrown if the source does not have a `.search` method, or if a method exists
            that does not support dictionary queries of the type provided to `self.search`.
        kwargs: dict
            Arguments/user parameters to use for opening the intake source. For example, many intake drivers support
            a `storage_options` argument with parameters to be passed to the backend file-system.`.

        Returns
        -------
        source: :py:class:`intake.DataSource`
            A dictionary of sources.
        """

        if len(self) == 1:
            res = self.to_source_dict(pass_query, **kwargs)
            _, source = res.popitem()
            return source
        else:
            raise ValueError(
                f"Expected exactly one source, received {len(self)}. Please refine your search or use "
                "`.to_source_dict()`."
            )

    @property
    def df(self) -> pd.DataFrame:
        """
        Return a pandas :py:class:`~pandas.DataFrame` representation of the dataframe catalog. This property is
        mostly for internal use. Users may find the `df_summary` property more useful.
        """
        return self._df

    @property
    def columns(self) -> list[str]:
        """
        Return a list of the columns in the dataframe catalog.
        """
        return self.df.columns.tolist()

    @property
    def columns_with_iterables(self) -> set[str]:
        """
        Return a list of the columns in the dataframe catalog that have iterables.
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
        Return a pandas :py:class:`~pandas.DataFrame` summary of unique entries in dataframe catalog.
        """

        if self._df.empty:
            self._df_summary = self.df.set_index(self.name_column).drop(
                columns=self.yaml_column
            )
        elif self._df_summary is None:
            self._df_summary = self.df.groupby(self.name_column).agg(
                {
                    col: lambda x: _find_unique(x, self.columns_with_iterables)
                    for col in self.df.columns.drop(
                        [self.name_column, self.yaml_column]
                    )
                },
            )

        return self._df_summary


def _find_unique(series, columns_with_iterables):
    """
    Return a set of unique values in a series
    """
    values = series.dropna()
    if series.name in columns_with_iterables:
        values = tlz.concat(values)
    return set(values)


def _columns_with_iterables(df, sample=False):
    """
    Return a list of the columns in the provided pandas dataframe/series that have iterables.
    Stolen from https://github.com/intake/intake-esm/blob/main/intake_esm/cat.py#L277
    """

    _df = df.sample(20, replace=True) if sample else df

    has_iterables = _df.map(type).isin([list, tuple, set]).any().to_dict()

    return [col for col, check in has_iterables.items() if check]
