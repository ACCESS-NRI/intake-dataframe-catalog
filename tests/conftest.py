# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import shutil
from pathlib import Path

from pytest import fixture

here = Path(__file__).parent


@fixture(scope="session")
def catalog_path(tmp_path_factory):
    """
    Fixture for moving the contents of test/data to a temporary directory
    """

    tmp_path = tmp_path_factory.mktemp("data")
    cat_path = here / Path("data/catalogs")
    # The following is a dodgy hack that seems to be related to pytest keeping
    # the temporary directory around after the test has completed. This needs to
    # be investigated further.
    try:
        shutil.copytree(cat_path, str(tmp_path))
    except FileExistsError:
        pass

    return tmp_path


@fixture
def source_path():
    return here / Path("data/source")
