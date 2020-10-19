# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml

from swh.scheduler.api.server import load_and_check_config


def prepare_config_file(tmpdir, content, name="config.yml"):
    """Prepare configuration file in `$tmpdir/name` with content `content`.

    Args:
        tmpdir (LocalPath): root directory
        content (str/dict): Content of the file either as string or as a dict.
                            If a dict, converts the dict into a yaml string.
        name (str): configuration filename

    Returns
        path (str) of the configuration file prepared.

    """
    config_path = tmpdir / name
    if isinstance(content, dict):  # convert if needed
        content = yaml.dump(content)
    config_path.write_text(content, encoding="utf-8")
    # pytest on python3.5 does not support LocalPath manipulation, so
    # convert path to string
    return str(config_path)


@pytest.mark.parametrize("scheduler_class", [None, ""])
def test_load_and_check_config_no_configuration(scheduler_class):
    """Inexistent configuration files raises"""
    with pytest.raises(EnvironmentError, match="Configuration file must be defined"):
        load_and_check_config(scheduler_class)


def test_load_and_check_config_inexistent_fil():
    """Inexistent config filepath should raise"""
    config_path = "/some/inexistent/config.yml"
    expected_error = f"Configuration file {config_path} does not exist"
    with pytest.raises(FileNotFoundError, match=expected_error):
        load_and_check_config(config_path)


def test_load_and_check_config_wrong_configuration(tmpdir):
    """Wrong configuration raises"""
    config_path = prepare_config_file(tmpdir, "something: useless")
    with pytest.raises(KeyError, match="Missing '%scheduler' configuration"):
        load_and_check_config(config_path)


def test_load_and_check_config_remote_config_local_type_raise(tmpdir):
    """Configuration without 'local' storage is rejected"""
    config = {"scheduler": {"cls": "remote"}}
    config_path = prepare_config_file(tmpdir, config)
    expected_error = (
        "The scheduler backend can only be started with a 'local'" " configuration"
    )
    with pytest.raises(ValueError, match=expected_error):
        load_and_check_config(config_path, type="local")


def test_load_and_check_config_local_incomplete_configuration(tmpdir):
    """Incomplete 'local' configuration should raise"""
    config = {"scheduler": {"cls": "local", "something": "needed-for-test",}}

    config_path = prepare_config_file(tmpdir, config)
    expected_error = "Invalid configuration; missing 'db' config entry"
    with pytest.raises(KeyError, match=expected_error):
        load_and_check_config(config_path)


def test_load_and_check_config_local_config_fine(tmpdir):
    """Local configuration is fine"""
    config = {"scheduler": {"cls": "local", "db": "db",}}
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path, type="local")
    assert cfg == config


def test_load_and_check_config_remote_config_fine(tmpdir):
    """Remote configuration is fine"""
    config = {"scheduler": {"cls": "remote"}}
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path, type="any")
    assert cfg == config
