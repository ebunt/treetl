
import json
import pkgutil
import logging


# maintain package version
__pkg_dat = pkgutil.get_data(__package__, 'pkg_info.json').decode('utf-8')
pkg_info = json.loads(__pkg_dat)
__version__ = pkg_info['version']

logger = logging.getLogger(__name__)
if len(logger.handlers) == 0:
    logger.addHandler(logging.NullHandler())

# core objects have the option to be imported at top level
from treetl.job import *
