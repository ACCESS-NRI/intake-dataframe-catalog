import os

import yaml
import fsspec
import pandas as pd

from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_dataframe_catalog import __version__


class DFCatalogValidationError(Exception):
    pass

class DFCatalogModel:
    """
    Model for a dataframe (DF) catalog of intake catalogs and associated metadata. The in-memory
    representation for the catalog is a Pandas DataFrame.
    """
    
    def __init__(
        self,
        name_column="name", 
        yaml_column="yaml",
        metadata_columns=None,
    ):
        """
        Parameters
        ----------
        name_column: str, optional
            Name of the column in the DF catalog containing the names of the intake catalogs
        yaml_column: str, optional
            Name of the column in the DF catalog containing intake yaml descriptions of the intake 
            catalogs
        metadata_columns: list of str, optional
            Names of additional columns in the DF catalog containing metadata for each of the intake 
            catalogs
        """
        
        self.name_column = name_column
        self.yaml_column = yaml_column
        self.metadata_columns = metadata_columns or []

        self._df = pd.DataFrame(
            columns=[self.name_column] + self.metadata_columns + [self.yaml_column]
        )
        
    @classmethod
    def load(cls, path, name_column="name", yaml_column="yaml", storage_options=None, **kwargs):
        """
        Load a DF catalog from a file
        
        Parameters
        ----------
        path: str
            Path to the DF catalog file.
        name_column: str, optional
            Name of the column in the dataframe containing the names of the intake catalogs.
        yaml_column: str, optional
            Name of the column in the dataframe containing intake yaml descriptions of the intake 
            catalogs.
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~dask.dataframe.read_csv`.
        """
        storage_options = storage_options or {}
        kwargs = kwargs or {}

        with fsspec.open(path, **storage_options) as fobj:
            df = pd.read_csv(fobj, **kwargs)
            
        metadata_columns = list(set(df.columns) - set([name_column, yaml_column]))
        cat = cls(name_column, yaml_column, metadata_columns)
        cat._df = df
        cat.validate()
        
        return cat
    
    def save(self, name, directory=None, storage_options=None, **kwargs):
        """
        Save a DF catalog to a file
        
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
        compression = csv_kwargs.get('compression')
        extensions = {"gzip": ".gz", "bz2": ".bz2", "zip": ".zip", "xz": ".xz", None: ""}
        fname = f"{fname}{extensions[compression]}"
        
        with fs.open(fname, 'wb') as fobj:
            self.df.to_csv(fobj, **csv_kwargs)
        
    def add(self, cat, metadata=None, overwrite=False):
        """
        Add an intake catalog to the DF catalog
        
        Parameters
        ----------
        cat: object 
            An intake catalog object with a .yaml() method
        metadata : dict, optional
            Dictionary of metadata associated with the intake catalog
        overwrite : bool, optional
            If True, overwrite all existing entries in the DF catalog with name_column entries that 
            match the name of this cat
        """
        metadata = metadata or {}
        data = metadata.copy()
        data[self.name_column] = cat.name
        data[self.yaml_column] = cat.yaml()
        row = pd.DataFrame(data, index=[0])
        
        if set(self.df.columns) == set(row.columns):
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
    
    def validate(self):
        """
        Validate a DF catalog
        """
        cols_avail = set(self.df.columns)
        cols_valid = set([self.name_column] + self.metadata_columns + [self.yaml_column])
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
        
    @property
    def df(self):
        """
        Return pandas :py:class:`~pandas.DataFrame` representation of the catalog.
        """
        return self._df
    
class DFFileCatalog(Catalog):
    """
    Manages a table of intake catalogs and associated metadata.
    """
    
    pversion = __version__
    container = "catalog"
    partition_access = None
    name = "dataframe_file_cat"
    
    def __init__(
        self,
        path,
        name_column="name", 
        yaml_column="yaml", 
        storage_options=None,
        read_kwargs=None,
        **intake_kwargs
    ):
        """
        Parameters
        ----------
        path : str
            Path to the DF catalog file.
        create_new: bool, optional
            If True, create a new file, overwriting any existing file with the same name
        name_column: str, optional
            Name of the column in the tabular file containing the names of the intake catalogs
        yaml_column: str, optional
            Name of the column in the tabular file containing intake yaml descriptions of the intake 
            catalogs
        storage_options : dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        read_kwargs : dict, optional
            Additional keyword arguments passed to :py:func:`~pd.DataFrame.read_csv` when reading from
            the DFFileCatalog.  
        intake_kwargs : dict, optional
            Additional keyword arguments to pass to the :py:class:`~intake.catalog.Catalog` base class.
        """
        
        self.path = path
        self.name_column = name_column
        self.yaml_column = yaml_column
        self.storage_options = storage_options or {}
        self._read_kwargs = read_kwargs or {}
        
        self._entries = {}
        self.dfcat = None
        
        super().__init__(**intake_kwargs)
        
    def keys(self):
        """
        Get keys for the catalog entries
        """
        return list(self.df[self.name_column])
    
    def __len__(self):
        return len(self.keys())
    
    def __getitem__(self, key: str):
        """
        Returns an intake catalog object for a given key
        """
        try:
            return self._entries[key]
        except KeyError as e:
            if key in self.keys():
                yamls = list(
                    self.dfcat.df.loc[self.dfcat.df[self.name_column] == key, self.yaml_column]
                )
                yaml_text = yamls[0]
                # If there are multiple entries with the same name, make sure they all point to
                # the same catalog
                if len(yamls) > 1:
                    assert all(y == yaml_text for y in yamls)
                    
                self._entries[key] = LocalCatalogEntry(
                    name=key, 
                    **yaml.safe_load(yaml_text)["sources"][key]
                ).get()
                return self._entries[key]
            raise KeyError(
                f'key={key} not found in catalog. You can access the list of valid keys via the .keys() method.'
            ) from e
        
    def _load(self):
        """
        Load the DF catalog from file.
        """
        self.dfcat = DFCatalogModel.load(
            self.path, 
            self.name_column, 
            self.yaml_column, 
            self.storage_options,
            **self._read_kwargs
        )
                
    @property
    def df(self):
        """
        Return pandas :py:class:`~pandas.DataFrame` representation of the catalog.
        """
        return self.dfcat.df