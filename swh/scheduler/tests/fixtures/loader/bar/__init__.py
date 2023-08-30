# Copyright (C) 2023 the Software Heritage developers
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class BarLoader:
    pass


def register():

    return {
        "loader": BarLoader,
        "task_modules": ["%s.tasks" % __name__],
    }
