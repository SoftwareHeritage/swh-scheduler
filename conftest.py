# Copyright (C) 2020-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from hypothesis import settings

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)


pytest_plugins = [
    "swh.scheduler.pytest_plugin",
    "swh.storage.pytest_plugin",
]

try:
    import swh.journal.pytest_plugin  # noqa

    pytest_plugins.append("swh.journal.pytest_plugin")
except ImportError:
    pass
