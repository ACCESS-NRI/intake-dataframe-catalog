from enum import Enum


class _DisplayType(Enum):
    JUPYTER_NOTEBOOK = 0
    IPYTHON_REPL = 1
    REGULAR_REPL = 2


class _DisplayOptions:
    """
    Singleton class to set display options for Pandas DataFrames.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "initialized"):
            return None
        self.set_pd_options()
        self.initialized = True

    def __str__(self):
        return f"DisplayOptions(display_type={self.display_type})"

    def __repr__(self):
        return str(self)

    def set_pd_options(self) -> None:
        """
        Set display.max_colwidth to 200 and max_rows to None - but only if we are
        in a Jupyter Notebook. Otherwise, leave the defaults.
        """

        if self.display_type == _DisplayType.JUPYTER_NOTEBOOK:
            import pandas as pd

            pd.set_option("display.max_colwidth", 200)
            pd.set_option("display.max_rows", None)

        return None

    @property
    def display_type(self) -> _DisplayType:
        try:
            # Check for Jupyter Notebook
            ipy = get_ipython()
            if hasattr(ipy, "kernel"):
                return _DisplayType.JUPYTER_NOTEBOOK
            elif hasattr(ipy, "config"):
                return _DisplayType.IPYTHON_REPL
        except NameError:
            return _DisplayType.REGULAR_REPL

    @property
    def is_notebook(self) -> bool:
        return self.display_type == _DisplayType.JUPYTER_NOTEBOOK


display_options = _DisplayOptions()
