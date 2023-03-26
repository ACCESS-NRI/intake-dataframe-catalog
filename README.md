# intake-dataframe-catalog

A simple intake driver for a searchable table of intake catalogs and associated metadata

Note, this is a work-in-progress. Expect things to change/break frequently and without warning.

## Contributing

This package is still in its very early stages of development. Contributions of all forms are very welcome. The following covers some general guidelines for maintainers and contributors.

#### Preparing Pull Requests
1. Fork this respository.

2. Clone your fork locally, connect your repository to the upstream (main project), and create a branch to work on:

```bash
$ git clone git@github.com:YOUR_GITHUB_USERNAME/intake-dataframe-catalog.git
$ cd intake-dataframe-catalog
$ git remote add upstream git@github.com:ACCESS-NRI/intake-dataframe-catalog.git
$ git checkout -b YOUR-BUGFIX-FEATURE-BRANCH-NAME main
```

3. Install `intake-dataframe-catalog`'s dependencies into a new conda environment:

```bash
$ conda env create -f environment-dev.yml
$ conda activate intake-df-cat-dev
```

4. Install `intake-dataframe-catalog` using the editable flag (meaning any changes you make to the package will be reflected directly in your environment without having to reinstall):

```bash
$ pip install --no-deps -e .
```

Aside: to have the changes you make to the package register immediately when running IPython (e.g. a Jupyter notebook), run the following magic commands in your IPython editor:

```python
%load_ext autoreload
%autoreload 2
```

5. This project uses `black` to format code and `flake8` for linting. We use `pre-commit` to ensure these have been run. Please set up commit hooks by running the following. This will mean that `black` and `flake8` are run whenever you make a commit:

```bash
pre-commit install
```

You can also run `pre-commit` manually at any point to format your code:

```bash
pre-commit run --all-files
 ```

6. Start making and committing your edits, including adding docstrings to functions and tests to `intake-dataframe-catalog/tests` to check that your contributions are doing what they're suppose to. Please try to follow [numpydoc style](https://numpydoc.readthedocs.io/en/latest/format.html) for docstrings. To run the test suite:

```bash
pytest intake-dataframe-catalog
```

#### Preparing a new release (not yet set up)

New releases to PyPI are published automatically when a tag is pushed to Github. To publish a new release:

```bash
export RELEASE=vX.X.X

# Create git tags
git commit --allow-empty -m "Release $RELEASE"
git tag -a $RELEASE -m "Version $RELEASE"

git push --tags
```
