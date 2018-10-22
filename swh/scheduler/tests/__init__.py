from os import path
import swh.scheduler


SQL_DIR = path.join(path.dirname(swh.scheduler.__file__), 'sql')
