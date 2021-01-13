# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import sys

from . import fill_test_data, run


def usage():
    print(f"Usage: {sys.argv[0]} fill_test_data/run")
    sys.exit(2)


def main(argv):
    try:
        myself, arg = argv
    except ValueError:
        usage()

    if arg == "run":
        run()
    elif arg == "fill_test_data":
        fill_test_data()
    else:
        usage()


if __name__ == "__main__":
    main(sys.argv)
