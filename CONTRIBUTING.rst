Contributor Guide
=================

This package is in its early stages of development. All contributions are welcome. You can help out just by using intake-dataframe-catalog and reporting
`issues <https://github.com/ACCESS-NRI/intake-dataframe-catalog/issues>`__.

The following sections cover some general guidelines for maintainers and
contributors wanting to help develop intake-dataframe-catalog.


Feature requests, suggestions and bug reports
---------------------------------------------

We are eager to hear about any bugs you have found, new features you
would like to see and any other suggestions you may have. Please feel
free to submit these as 
`issues <https://github.com/ACCESS-NRI/intake-dataframe-catalog/issues>`__.

When suggesting features, please make sure to explain in detail how
the proposed feature should work and to keep the scope as narrow as
possible. This makes features easier to implement in small PRs.

When report bugs, please include:

* Any details about your local setup that might be helpful in
  troubleshooting, specifically the Python interpreter version, installed
  libraries, and intake-dataframe-catalog version.
* Detailed steps to reproduce the bug, ideally a `Minimal, Complete and
  Verifiable Example <http://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports>`__.
* If possible, a demonstration test that currently fails but should pass
  when the bug is fixed.


Writing documentation
---------------------
Adding documentation is always helpful. This may include:

* More complementary documentation. Have you perhaps found something unclear?
* Docstrings.
* Example notebooks of intake-dataframe-catalog being used in real analyses.

The intake-dataframe-catalog documentation is written in reStructuredText. You
can follow the conventions in already written documents. Some helpful guides
can be found
`here <http://docutils.sourceforge.net/docs/user/rst/quickref.html>`__ and
`here <https://github.com/ralsina/rst-cheatsheet/blob/master/rst-cheatsheet.rst>`__.

Contributions to documentation should be submitted via pull requests to the
intake-dataframe-catalog core repository. Follow the :ref:`steps below<pull_requests>`, 
replacing step 3 with the following::

    $ conda env create -f docs/environment-doc.yml
    $ conda activate intake-df-cat-doc

When writing and editing documentation, it can be useful to see the resulting
build without having to push to Github. You can build the documentation locally
by running::

    $ cd docs/
    $ make html

This will build the documentation locally in ``doc/_build/``. You can then open
``_build/html/index.html`` in your web browser to view the documentation.

.. _pull_requests:

Preparing Pull Requests
-----------------------
#. Fork the
   `intake-dataframe-catalog GitHub repository 
   <https://github.com/ACCESS-NRI/intake-dataframe-catalog>`__.  It's fine to 
   use "intake-dataframe-catalog" as your fork repository name because it will live
   under your username.

#. Clone your fork locally, connect your repository to the upstream (main
   project), and create a branch to work on::

    $ git clone git@github.com:YOUR_GITHUB_USERNAME/intake-dataframe-catalog.git
    $ cd intake-dataframe-catalog
    $ git remote add upstream git@github.com:ACCESS-NRI/intake-dataframe-catalog.git
    $ git checkout -b your-bugfix-feature-branch-name master

   If you need some help with Git, follow
   `this quick start guide <https://git.wiki.kernel.org/index.php/QuickStart>`__

#. Install dependencies into a new conda environment::

    $ conda env create -f ci/environment-3.11.yml
    $ conda activate intake-df-cat-test

#. Install intake-dataframe-catalog using the editable flag (meaning any changes you 
   make to the package will be reflected directly in your environment)::

    $ pip install --no-deps -e .

#. Start making your edits. Please try to type annotate your additions as
   much as possible. Adding type annotations to existing unannotated code is
   also very welcome. You can read about Python typing
   `here <https://mypy.readthedocs.io/en/stable/getting_started.html#function-signatures-and-dynamic-vs-static-typing>`__.

#. Break your edits up into reasonably sized commits::

    $ git commit -a -m "<commit message>"
    $ git push -u

   It can be useful to manually run `pre-commit <https://pre-commit.com>`_ as you
   make your edits. ``pre-commit`` will run checks on the format and typing of
   your code and will show you where you need to make changes. This will mean
   your code is more likely to pass the CI checks when you push it::

    $ pre-commit run --all-files

#. Run the tests (including those you add to test your edits!)::

    $ pytest .

#. Add a new entry describing your contribution to the :ref:`changelog`
   in :code:`doc/reference/changelog.rst`. Please try to follow the format of the existing
   entries.

#. Submit a pull request through the GitHub `website <https://github.com/ACCESS-NRI/intake-dataframe-catalog/pulls>`__.

   Note that you can create the Pull Request while you're working on your PR.
   The PR will update as you add more commits. intake-dataframe-catalog developers and
   contributors can then review your code and offer suggestions.

Preparing a new release
-----------------------

New releases to PyPI and conda are published automatically when a tag is pushed to Github. The preferred approach is to 
trigger this process by creating a new tag and corresponding release on GitHub.

#. Go to https://github.com/ACCESS-NRI/intake-dataframe-catalog

#. Click on "Create new release" on the right-hand side of the screen

#. Enter the new version (vX.X.X) as the tag and release title. Add a brief description of the release.

#. Click on "Publish release". This should create the release on GitHub and trigger the workflow that builds and uploads 
   the new version to PyPI and conda

Alternatively (any discouraged), to trigger a new release from the command line::

    $ git fetch --all --tags
    $ export RELEASE=vX.X.X
    $ git commit --allow-empty -m "Release $RELEASE"
    $ git tag -a $RELEASE -m "Version $RELEASE"
    $ git push --tags
