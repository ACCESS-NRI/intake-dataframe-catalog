import fsspec

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
    def load(cls, urlpath, name_column="name", yaml_column="yaml", storage_options=None, **kwargs):
        """
        Load a DF catalog from a file
        
        Parameters
        ----------
        urlpath: str
            Path to the DF catalog file. May be a local path, or remote path if including a protocol
            specifier such as ``'s3://'``.
        name_column: str, optional
            Name of the column in the dataframe containing the names of the intake catalogs
        yaml_column: str, optional
            Name of the column in the dataframe containing intake yaml descriptions of the intake 
            catalogs
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~dask.dataframe.read_csv`
        """
        storage_options = storage_options or {}
        kwargs = kwargs or {}
        _mapper = fsspec.get_mapper(urlpath, **storage_options)

        with fsspec.open(urlpath, **storage_options) as fobj:
            df = pd.read_csv(fobj, **kwargs)
            
        metadata_columns = list(set(df.columns) - set([name_column, yaml_column]))
        cat = cls(name_column, yaml_column, metadata_columns)
        cat._df = df
        cat.validate()
        
        return cat
    
    def save(urlpath, storage_options=None, **kwargs):
        """
        Save a DF catalog to a file
        
        Parameters
        ----------
        urlpath: str
            Path to the DF catalog file. May be a local path, or remote path if
            including a protocol specifier such as ``'s3://'``.
        storage_options: dict, optional
            Any parameters that need to be passed to the remote data backend, such as credentials.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~dask.dataframe.to_csv`
        """
        
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