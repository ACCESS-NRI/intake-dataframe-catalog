import typing
import warnings

import pydantic

import pandas as pd

from intake.catalog import Catalog

from intake_esm.cat import ESMCatalogModel
from intake_esm.core import esm_datastore


class MetaDatastoreError(Exception):
    pass


class meta_esm_datastore(Catalog):
    """
    An intake plugin for parsing a meta-esm catalog and loading assets into intake-esm catalogs.
    The in-memory representation for the meta-esm catalog is a Pandas DataFrame.

    Parameters
    ----------
    obj : str, dict
        If string, this must be a path or URL to a meta-esm catalog JSON file.
        If dict, this must be a dict representation of a meta-esm catalog.
        This dict must have two keys: 'esmcat' and 'df'. The 'esmcat' key must be a
        dict representation of the meta-esm catalog and the 'df' key must
        be a Pandas DataFrame containing content that would otherwise be in a CSV file.
    sep : str, optional
        Delimiter to use when constructing a key for a query, by default '.'
    read_csv_kwargs : dict, optional
        Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.
    storage_options : dict, optional
        Parameters passed to the backend file-system such as Google Cloud Storage,
        Amazon Web Service S3.
    intake_kwargs: dict, optional
        Additional keyword arguments are passed through to the :py:class:`~intake.catalog.Catalog` base class.
    """

    name = "meta_esm_datastore"
    container = "catalog"

    def __init__(
        self,
        obj: typing.Union[pydantic.FilePath, pydantic.AnyUrl, dict[str, typing.Any]],
        *,
        sep: str = ".",
        read_csv_kwargs: dict[str, typing.Any] = None,
        storage_options: dict[str, typing.Any] = None,
        **intake_kwargs: dict[str, typing.Any],
    ):
        """Intake catalog representing a meta-esm collection."""
        super().__init__(**intake_kwargs)
        self.sep = sep
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.storage_options = storage_options or {}

        if isinstance(obj, dict):
            self.esmcat = ESMCatalogModel.from_dict(obj)
        else:
            self.esmcat = ESMCatalogModel.load(
                obj,
                storage_options=self.storage_options,
                read_csv_kwargs=read_csv_kwargs,
            )

        self._entries = {}
        self.esm_datastores = {}

    def keys(self) -> list[str]:
        """
        Get keys for the catalog entries

        Returns
        -------
        list
            keys for the catalog entries
        """
        return list(self.esmcat._construct_group_keys(sep=self.sep).keys())

    def keys_info(self) -> pd.DataFrame:
        """
        Get keys for the catalog entries and their metadata

        Returns
        -------
        pandas.DataFrame
            keys for the catalog entries and their metadata

        Examples
        --------

        >>> import intake
        >>> cat = intake.open_meta_esm_datastore("./tests/sample-catalogs/cesm1-lens-netcdf.json")
        >>> cat.keys_info()
                        component experiment stream
        key
        ocn.20C.pop.h         ocn        20C  pop.h
        ocn.CTRL.pop.h        ocn       CTRL  pop.h
        ocn.RCP85.pop.h       ocn      RCP85  pop.h



        """
        results = self.esmcat._construct_group_keys(sep=self.sep)
        data = {
            key: dict(zip(self.esmcat.aggregation_control.groupby_attrs, results[key]))
            for key in results
        }
        data = pd.DataFrame.from_dict(data, orient="index")
        data.index.name = "key"
        return data

    @property
    def key_template(self) -> str:
        """
        Return string template used to create catalog entry keys

        Returns
        -------
        str
          string template used to create catalog entry keys
        """
        if self.esmcat.aggregation_control.groupby_attrs:
            return self.sep.join(self.esmcat.aggregation_control.groupby_attrs)
        else:
            return self.sep.join(self.esmcat.df.columns)

    @property
    def df(self) -> pd.DataFrame:
        """
        Return pandas :py:class:`~pandas.DataFrame` representation of the meta-esm catalog.
        """
        return self.esmcat.df

    def __len__(self) -> int:
        return len(self.keys())

    def _get_entries(self) -> dict[str, esm_datastore]:
        # Due to just-in-time entry creation, we may not have all entries loaded
        # We need to make sure to create entries missing from self._entries
        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]
        return self._entries

    @pydantic.validate_arguments
    def __getitem__(self, key: str) -> esm_datastore:
        """
        This method takes a unique key argument according to groupby_attr and returns the
        corresponding intake-esm datastore.

        Parameters
        ----------
        key : str
          key to use for catalog entry lookup

        Returns
        -------
        intake_esm.core.esm_datastore
             An intake-esm datastore

        Raises
        ------
        KeyError
            if key is not found.

        Examples
        --------
        >>> meta_cat = intake.open_meta_esm_datastore("meta.json")
        >>> exp_cat = meta_cat["my_model.my_experiment"]
        """
        # The canonical unique key is the key of a compatible group of assets
        try:
            return self._entries[key]
        except KeyError as e:
            if key in self.keys():
                keys_dict = self.esmcat._construct_group_keys(sep=self.sep)
                grouped = self.esmcat.grouped

                internal_key = keys_dict[key]

                if isinstance(grouped, pd.DataFrame):
                    records = [grouped.loc[internal_key].to_dict()]
                else:
                    records = grouped.get_group(internal_key).to_dict(orient="records")

                catalog_files = [
                    record[self.esmcat.assets.column_name] for record in records
                ]

                if len(set(catalog_files)) != 1:
                    raise MetaDatastoreError(
                        f"Unique key {internal_key} refers to multiple ESM datastores"
                    )

                entry = esm_datastore(
                    catalog_files[0],
                    sep=self.sep,
                    storage_options=self.storage_options,
                    # read_csv_kwargs=self.read_csv_kwargs
                )

                self._entries[key] = entry
                return self._entries[key]
            raise KeyError(
                f"key={key} not found in catalog. You can access the list of valid keys via the .keys() method."
            ) from e

    def __contains__(self, key) -> bool:
        # Python falls back to iterating over the entire catalog if this method is not defined. To avoid this,
        # we implement it differently
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __repr__(self) -> str:
        """Make string representation of object."""
        return f'<{self.esmcat.id or ""} catalog with {len(self)} experiment(s) from {len(self.df)} asset(s)>'

    def _repr_html_(self) -> str:
        """
        Return an html representation for the catalog object.
        Mainly for IPython notebook
        """
        uniques = pd.DataFrame(self.nunique(), columns=["unique"])
        text = uniques._repr_html_()
        return (
            f'<p><strong>{self.esmcat.id or ""} catalog with {len(self)} experiment(s)'
            f"from {len(self.df)} asset(s)</strong>:</p> {text}"
        )

    def _ipython_display_(self):
        """
        Display the entry as a rich object in an IPython session
        """
        from IPython.display import HTML, display

        contents = self._repr_html_()
        display(HTML(contents))

    def __dir__(self) -> list[str]:
        rv = [
            "df",
            "keys",
            "keys_info",
            "serialize",
            "datasets",
            "search",
            "unique",
            "nunique",
            "key_template",
            "to_esm_datastore",
            "to_esm_datastore_dict",
        ]
        return sorted(list(self.__dict__.keys()) + rv)

    def _ipython_key_completions_(self):
        return self.__dir__()

    @pydantic.validate_arguments
    def search(
        self, require_all_on: typing.Union[str, list[str]] = None, **query: typing.Any
    ):
        """Search for entries in the catalog.

        Parameters
        ----------
        require_all_on : list, str, optional
            A dataframe column or a list of dataframe columns across
            which all entries must satisfy the query criteria.
            If None, return entries that fulfill any of the criteria specified
            in the query, by default None.
        **query:
            keyword arguments corresponding to user's query to execute against the dataframe.

        Returns
        -------
        cat : :py:class:`~intake_meta_esm.core.meta_esm_datastore`
          A new Catalog with a subset of the entries in this Catalog.

        Examples
        --------
        >>> import intake
        >>> meta_cat = intake.open_meta_esm_datastore("meta.json")
        >>> sub_cat = cat.search(
        ...     model="access-om2",
        ...     variable="sst",
        ... )
        """

        esmcat_results = self.esmcat.search(require_all_on=require_all_on, query=query)

        cat = self.__class__({"esmcat": self.esmcat.dict(), "df": esmcat_results})
        cat.esmcat.catalog_file = None  # Don't save the catalog file
        return cat

    @pydantic.validate_arguments
    def serialize(
        self,
        name: pydantic.StrictStr,
        directory: typing.Union[pydantic.DirectoryPath, pydantic.StrictStr] = None,
        catalog_type: str = "dict",
        to_csv_kwargs: dict[typing.Any, typing.Any] = None,
        json_dump_kwargs: dict[typing.Any, typing.Any] = None,
        storage_options: dict[str, typing.Any] = None,
    ) -> None:
        """Serialize catalog to corresponding json and csv files.

        Parameters
        ----------
        name : str
            name to use when creating ESM catalog json file and csv catalog.
        directory : str, PathLike, default None
            The path to the local directory. If None, use the current directory
        catalog_type: str, default 'dict'
            Whether to save the catalog table as a dictionary in the JSON file or as a separate CSV file.
        to_csv_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:meth:`~pandas.DataFrame.to_csv` method.
        json_dump_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:func:`~json.dump` function.
        storage_options: dict
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.

        Notes
        -----
        Large catalogs can result in large JSON files. To keep the JSON file size manageable, call with
        `catalog_type='file'` to save catalog as a separate CSV file.

        Examples
        --------
        >>> import intake
        >>> meta_cat = intake.open_meta_esm_datastore("meta.json")
        >>> sub_cat = cat.search(
        ...     model="access-om2",
        ...     variable="sst",
        ... )
        >>> sub_cat.serialize(name="access_om2_sst", catalog_type="file")
        """

        self.esmcat.save(
            name,
            directory=directory,
            catalog_type=catalog_type,
            to_csv_kwargs=to_csv_kwargs,
            json_dump_kwargs=json_dump_kwargs,
            storage_options=storage_options,
        )

    def nunique(self) -> pd.Series:
        """Count distinct observations across dataframe columns
        in the catalog.
        """
        nunique = self.esmcat.nunique()
        return nunique

    def unique(self) -> pd.Series:
        """Return unique values for given columns in the
        catalog.
        """
        unique = self.esmcat.unique()
        return unique

    @pydantic.validate_arguments
    def to_esm_datastore_dict(self) -> dict[str, esm_datastore]:
        """
        Load intake-esm catalog(s) for the experiment(s) in this meta-esm catalog.
        """
        if not self.keys():
            warnings.warn(
                "There are no experiments to return catalogs for! Returning an empty dictionary.",
                UserWarning,
                stacklevel=2,
            )
            return {}

        self.esm_datastores = {key: cat for key, cat in self.items()}
        return self.esm_datastores

    def to_esm_datastore(self, **kwargs) -> esm_datastore:
        """
        Load an intake-esm catalog for the experiment in this meta-esm catalog.

        This is only possible if the search returned exactly one result.

        Parameters
        ----------
        kwargs: dict
          Parameters forwarded to :py:func:`~intake_esm.esm_datastore.to_dataset_dict`.

        Returns
        -------
        :py:class:`~xarray.Dataset`
        """
        if len(self) != 1:  # quick check to fail more quickly if there are many results
            raise ValueError(
                f"Expected exactly one experiment. Received {len(self)} experiments."
                "Please refine your search or use `.to_esm_datastore_dict()`."
            )
        _, cat = self.to_esm_datastore_dict().popitem()
        return cat
