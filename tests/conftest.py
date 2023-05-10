# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from distutils import dir_util
from pathlib import Path

from pytest import fixture

here = os.path.abspath(os.path.dirname(__file__))


@fixture(scope="session")
def catalog_path(tmp_path_factory):
    """
    Fixture for moving the contents of test/data to a temporary directory
    """

    tmp_path = tmp_path_factory.mktemp("data")
    cat_path = os.path.join(here, "data/catalogs")
    dir_util.copy_tree(cat_path, str(tmp_path))

    return tmp_path


@fixture
def source_path():
    return Path(os.path.join(here, "data/source"))
