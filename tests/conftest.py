import os
from distutils import dir_util
from pytest import fixture


@fixture
def datadir(tmpdir, request):
    """
    Fixture for moving the contents of testdata to a temporary directory
    """
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))

    if os.path.isdir(data_dir):
        dir_util.copy_tree(data_dir, str(tmpdir))

    return tmpdir
