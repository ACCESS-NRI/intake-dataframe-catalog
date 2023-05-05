from setuptools import find_packages, setup
import versioneer

setup(
    name="intake_dataframe_catalog",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="ACCESS-NRI",
    url="https://github.com/ACCESS-NRI/intake-dataframe-catalog",
    description="An intake driver for a searchable table of intake catalogs and associated metadata",
    long_description=(
        "intake-dataframe-catalog is a simple intake plugin for a searchable table of intake catalogs. "
        "The table is represented in memory as a pandas DataFrame and can be serialized and shared as "
        "a CSV file. Each row in the dataframe catalog corresponds to another intake catalog (refered "
        "to in this documentation as a 'subcatalog') and the columns contain metadata associated with "
        "each subcatalog that a user may want to peruse and/or search. The original use-case for "
        "intake-dataframe-catalog was to provide a user-friendly catalog of a large number "
        "`intake-esm <https://intake-esm.readthedocs.io/en/stable/>`_ catalogs. intake-dataframe-catalog "
        "enables users to peruse and search on core metadata from each intake-esm subcatalog to find "
        "the subcatalogs that are most relevant to their work (e.g. 'which subcatalogs contain model "
        "X and variable Y?'). Once a users has found the subcatalog(s) that interest them, they can "
        "load those subcatalogs and access the data they reference."
    ),
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "intake",
        "pandas",
    ],
    entry_points={
        "intake.drivers": [
            "df_catalog = intake_dataframe_catalog.core:DfFileCatalog",
        ]
    },
)
