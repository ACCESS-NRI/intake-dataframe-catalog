# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import tlz
import typing
import warnings

import yaml
import fsspec
import pandas as pd

import intake
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__
from ._search import search, search_apply_require_all_on


class DFCatalogValidationError(Exception):
    pass


class DFCatalogModel:
    """
    Model for a dataframe (DF) catalog of intake catalogs and associated metadata. This is the workhorse for
    writing/editing/extending DF catalogs to be read by DFFileCatalog. The in-memory representation for the
    catalog is a Pandas DataFrame.
    """

    def __init__(
        self,
        yaml_column: str = "yaml",
        name_column: str = "name",
        metadata_columns: list[str] = None,
    ):
        """
        Parameters
        ----------
        yaml_column: str, optional
            Name of the column in the DF catalog containing intake yaml descriptions of the intake
            catalogs.
        name_column: str, optional
            Name of the column in the DF catalog containing the names of the intake catalogs.
        metadata_columns: list of str, optional
            Names of additional columns in the DF catalog containing metadata for each of the intake
            catalogs.
        """

        self.yaml_column = yaml_column
        self.name_column = name_column
        self.metadata_columns = metadata_columns or []
        self._valid_columns = (
            [self.name_column] + self.metadata_columns + [self.yaml_column]
        )

        self._df = pd.DataFrame(columns=self._valid_columns)

    @classmethod
    def load(
        cls,
        path: str,
        yaml_column: str = "yaml",
        name_column: str = "name",
        storage_options: dict[str, typing.Any] = None,
        **kwargs: dict[str, typing.Any],
    ) -> "DFCatalogModel":
        """
        Load a DF catalog from a file.

        Parameters
        ----------
        path: str
            Path to the DF catalog file.
        yaml_column: str, optional
            Name of the column in the dataframe containing intake yaml descriptions of the intake
            catalogs.
        name_column: str, optional
            Name of the column in the dataframe containing the names of the intake catalogs.
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~dask.dataframe.read_csv`.

        Returns
        -------
        catalog: DFCatalogModel
            A DF catalog.
        """

        storage_options = storage_options or {}
        kwargs = kwargs or {}

        with fsspec.open(path, **storage_options) as fobj:
            df = pd.read_csv(fobj, **kwargs)

        metadata_columns = list(set(df.columns) - set([yaml_column, name_column]))
        cat = cls(yaml_column, name_column, metadata_columns)
        cat._df = df
        cat.validate()

        return cat

    @classmethod
    def from_dict(
        cls,
        entries: dict[list, typing.Any],
        cat_key: str,
        yaml_column: str = "yaml",
        name_column: str = "name",
    ) -> "DFCatalogModel":
        """
        Create a DF catalog from the given set of entries in dictionary format.

        Parameters
        ----------
        entries : dict
            Dictionary with at least a key containing a list of intake catalog objects. Additional
            metadata can be included in corresponding lists in other keys.
        cat_key : str, optional
            The key in the dictionary corresponding to the intake catalog object.
        yaml_column: str, optional
            Name of the column in the dataframe containing intake yaml descriptions of the intake
            catalogs.
        name_column: str, optional
            Name of the column in the dataframe containing the names of the intake catalogs.

        Returns
        -------
        catalog: DFCatalogModel
            A DF catalog.
        """

        cats = entries.pop(cat_key)
        metadata_columns = list(entries.keys())
        entries[yaml_column] = [cat.yaml() for cat in cats]
        entries[name_column] = [cat.name for cat in cats]

        cat = cls(yaml_column, name_column, metadata_columns)
        cat._df = pd.DataFrame(entries)
        cat.validate()

        return cat

    def save(
        self,
        name: str,
        directory: str = None,
        storage_options: dict[str, typing.Any] = None,
        **kwargs: dict[str, typing.Any],
    ) -> None:
        """
        Save a DF catalog to a file.

        Parameters
        ----------
        name: str
            Name of the DF catalog file.
        directory: str
            The directory or cloud storage bucket to save the DF catalog to. If None, use the
            current directory.
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~dask.dataframe.to_csv`.
        """

        if directory is None:
            directory = os.getcwd()

        mapper = fsspec.get_mapper(f"{directory}", storage_options=storage_options)
        fs = mapper.fs
        fname = f"{mapper.fs.protocol}://{mapper.root}/{name}.csv"

        csv_kwargs = {"index": False}
        csv_kwargs.update(kwargs or {})
        compression = csv_kwargs.get("compression")
        extensions = {
            "gzip": ".gz",
            "bz2": ".bz2",
            "zip": ".zip",
            "xz": ".xz",
            None: "",
        }
        fname = f"{fname}{extensions[compression]}"

        with fs.open(fname, "wb") as fobj:
            self.df.to_csv(fobj, **csv_kwargs)

    def add(
        self,
        cat: intake.DataSource,
        metadata: dict[str, typing.Any] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Add an intake catalog to the DF catalog.

        Parameters
        ----------
        cat: object
            An intake DataSource object (or child thereof) with a .yaml() method.
        metadata : dict, optional
            Dictionary of metadata associated with the intake catalog.
        overwrite : bool, optional
            If True, overwrite all existing entries in the DF catalog with name_column entries that
            match the name of this cat.
        """

        metadata = metadata or {}
        data = metadata.copy()
        data[self.yaml_column] = cat.yaml()
        data[self.name_column] = cat.name
        row = pd.DataFrame(data, index=[0])

        if set(self.columns) == set(row.columns):
            if overwrite:
                self._df.loc[self._df[self.name_column] == data[self.name_column]] = row
                self._df = self._df.dropna()
            else:
                self._df = pd.concat([self._df, row], ignore_index=True)
        else:
            raise DFCatalogValidationError(
                f"metadata must include the following keys to be added to this DF catalog: {self.metadata_columns}. "
                f"You passed a dictionary with the following keys: {list(metadata.keys())}"
            )

        self.validate()

    def validate(self) -> None:
        """
        Validate a DF catalog.
        """

        cols_avail = set(self.columns)
        cols_valid = set(self._valid_columns)
        invalid_cols = cols_avail - cols_valid
        missing_cols = cols_valid - cols_avail

        if invalid_cols:
            raise DFCatalogValidationError(
                f"The following columns are invalid for this DF catalog: {invalid_cols}. "
                f"Valid column names are {cols_valid}."
            )

        if missing_cols:
            raise DFCatalogValidationError(
                f"The following columns are missing for this DF catalog: {missing_cols}. "
                f"Available column names are {cols_avail}."
            )

        # Reorder columns for readability
        self._df = self._df[self._valid_columns]

    def search(self, require_all: bool = False, **query: typing.Any) -> pd.DataFrame:
        """
        Search for entries in the catalog.

        Parameters
        ----------
        query: dict, optional
            A dictionary of query parameters to execute against the DF catalog.
        require_all : bool, optional
            If True, entries must satisfy all the query criteria, otherwise results that satisfy any of criteria
            are returned.

        Returns
        -------
        dataframe: :py:class:`~pandas.DataFrame`
            A new dataframe with the entries satisfying the query criteria.
        """

        columns = self.columns

        for key, value in query.items():
            if key not in columns:
                raise ValueError(f"Column {key} not in columns {columns}")
            if not isinstance(query[key], list):
                query[key] = [query[key]]

        results = search(
            df=self.df, query=query, columns_with_iterables=self.columns_with_iterables
        )
        if require_all and not results.empty:
            results = search_apply_require_all_on(
                df=results,
                query=query,
                require_all_on=self.name_column,
                columns_with_iterables=self.columns_with_iterables,
            )

        return results

    def _unique(self) -> dict:
        def _find_unique(series):
            values = series.dropna()
            if series.name in self.columns_with_iterables:
                values = tlz.concat(values)
            return list(tlz.unique(values))

        data = self.df[self.df.columns]
        if data.empty:
            return {col: [] for col in self.df.columns}
        else:
            return data.apply(_find_unique, result_type="reduce").to_dict()

    def unique(self) -> pd.Series:
        """
        Return a series of unique values for each column in the DF catalog.
        """
        return pd.Series(self._unique())

    def nunique(self) -> pd.Series:
        """
        Return a series of the number of unique values for each column in the DF catalog.
        """
        return pd.Series(tlz.valmap(len, self._unique()))

    @property
    def df(self) -> pd.DataFrame:
        """
        Return :py:class:`~pandas.DataFrame` representation of the catalog.
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
        Return a set of the columns in the DF catalog that have iterables.
        """

        if self._df.empty:
            return set()
        has_iterables = (
            self._df.sample(20, replace=True)
            .applymap(type)
            .isin([list, tuple, set])
            .any()
            .to_dict()
        )

        return {column for column, check in has_iterables.items() if check}


class DFFileCatalog(Catalog):
    """
    Manages a table of intake catalogs and associated metadata.
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
            catalogs.
        name_column: str, optional
            Name of the column in the tabular file containing the names of the intake catalogs.
        storage_options : dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        read_kwargs : dict, optional
            Additional keyword arguments passed to :py:func:`~pd.DataFrame.read_csv` when reading from
            the DFFileCatalog.
        intake_kwargs : dict, optional
            Additional keyword arguments to pass to the :py:class:`~intake.catalog.Catalog` base class.
        """

        self.path = path
        self.yaml_column = yaml_column
        self.name_column = name_column
        self.storage_options = storage_options or {}
        self._read_kwargs = read_kwargs or {}

        self._entries = {}
        self.dfcat = DFCatalogModel(self.yaml_column, self.name_column)

        super().__init__(**intake_kwargs)

    def _load(self) -> None:
        """
        Load the DF catalog from file.
        """
        if self.path:
            self.dfcat = DFCatalogModel.load(
                self.path,
                self.yaml_column,
                self.name_column,
                self.storage_options,
                **self._read_kwargs,
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
                    self.dfcat.df.loc[
                        self.dfcat.df[self.name_column] == key, self.yaml_column
                    ]
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
                f"key={key} not found in catalog. You can access the list of valid keys via the .keys() method."
            ) from e

    def __repr__(self) -> str:
        return f"<Dataframe catalog with {len(self)} sub-catalogs(s) and {len(self.df)} rows>"

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
                "There are no subcatalogs to open! Returning an empty dictionary.",
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

    def serialize(
        self,
        name: str,
        directory: str = None,
        storage_options: dict[str, typing.Any] = None,
        **kwargs: dict[str, typing.Any],
    ) -> None:
        """
        Serialize the DF catalog to a file.

        Parameters
        ----------
        name: str
            Name of the DF catalog file.
        directory: str
            The directory or cloud storage bucket to save the DF catalog to. If None, use the
            current directory.
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~dask.dataframe.to_csv`.
        """
        self.dfcat.save(
            name, directory=directory, storage_options=storage_options, **kwargs
        )

    save = serialize

    def search(self, require_all: bool = False, **query: typing.Any) -> "DFFileCatalog":
        """
        Search for entries in the catalog.

        Parameters
        ----------
        query: dict, optional
            A dictionary of query parameters to execute against the DF catalog.
        require_all : bool, optional
            If True, entries must satisfy all the query criteria, otherwise results that satisfy any of criteria
            are returned.

        Returns
        -------
        catalog: DFFileCatalog
            A new catalog with the entries satisfying the query criteria.
        """

        dfcat_results = self.dfcat.search(require_all, **query)

        cat = self.__class__()
        cat.dfcat._df = dfcat_results

        return cat

    @property
    def df(self) -> pd.DataFrame:
        """
        Return :py:class:`~pandas.DataFrame` representation of the catalog.
        """
        return self.dfcat.df

    def keys(self) -> list[str]:
        """
        Return a list of keys for the catalog entries.
        """
        return self.unique()[self.name_column]

    def nunique(self) -> pd.Series:
        """
        Return a series of the number of unique values for each column in the DF catalog.
        """
        return self.dfcat.nunique()

    def unique(self) -> pd.Series:
        """
        Return a series of unique values for each column in the DF catalog.
        """
        return self.dfcat.unique()

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
