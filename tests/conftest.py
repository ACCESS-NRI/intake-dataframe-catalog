import os
from distutils import dir_util
from pytest import fixture


@fixture(scope="session")
def data_path(tmp_path_factory):
    """
    Fixture for moving the contents of test/data to a temporary directory
    """

    tmp_path = tmp_path_factory.mktemp("data")
    data_path = os.path.join(os.path.dirname(__file__), "data")
    dir_util.copy_tree(data_path, str(tmp_path))
    for f in os.walk(tmp_path):
        print(f)

    return tmp_path
