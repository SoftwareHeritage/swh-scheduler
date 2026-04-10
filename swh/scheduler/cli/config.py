# Copyright (C) 2024-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING

# WARNING: do not import unnecessary things here to keep cli startup time under
# control


if TYPE_CHECKING:
    from typing import Any, Dict, Optional


def read_config(config_file: Optional[Any] = None) -> Dict:
    """Read configuration from config_file if provided or from
    the SWH_CONFIG_FILENAME if set.
    """
    from os import environ

    from swh.core import config

    if config_file:
        conf = config.read(config_file)
    elif "SWH_CONFIG_FILENAME" in environ:
        conf = config.load_from_envvar()
    else:
        conf = {}
    return conf
