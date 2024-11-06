from intake_dataframe_catalog._display import _DisplayOptions


def test_display_opts_singleton():
    opts1 = _DisplayOptions()
    opts2 = _DisplayOptions()
    assert opts1 is opts2


# Create a test that checks if get_ipython() has a kernel attribute, then the display_type should be JUPYTER_NOTEBOOK
# If get_ipython() has a config attribute, then the display_type should be IPYTHON_REPL
# If get_ipython() raises a NameError, then the display_type should be REGULAR_REPL
