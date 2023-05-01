import os
from distutils import dir_util
from pytest import fixture


@fixture(scope="session")
def datadir(tmp_path_factory):
    """
    Fixture for moving the contents of testdata to a temporary directory
    """

    tmpdir = tmp_path_factory.mktemp("data")
    datadir = os.path.join(os.path.dirname(__file__), "data")
    dir_util.copy_tree(datadir, str(tmpdir))

    return tmpdir
