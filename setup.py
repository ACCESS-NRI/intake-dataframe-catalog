from setuptools import find_packages, setup
import versioneer

setup(
    name="intake_dataframe_catalog",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="ACCESS-NRI",
    url="https://github.com/ACCESS-NRI/intake-dataframe-catalog",
    description="An intake driver for a searchable table of intake catalogs and associated metadata",
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
